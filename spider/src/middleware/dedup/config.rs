use std::collections::HashSet;
use std::time::Duration;

use crate::middleware::Spec;

use super::{Fingerprint, invalid};

#[derive(Clone, Debug)]
pub struct Config {
    paths: Vec<String>,
    normalize: Normalize,
    ttl: Ttl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Ttl {
    Permanent,
    Disabled,
    Finite(Duration),
}

#[derive(Clone, Debug)]
pub(super) struct Normalize {
    enabled: bool,
    drop_query: HashSet<String>,
    sort_query: bool,
}

impl Config {
    pub fn from_spec(spec: &Spec) -> Result<Self, crate::middleware::Error> {
        if spec
            .hook
            .as_deref()
            .is_some_and(|hook| hook != "before_scheduler")
        {
            return Err(invalid("hook must be before_scheduler"));
        }
        let args = spec
            .args
            .as_object()
            .ok_or_else(|| invalid("args must be an object"))?;
        if args
            .keys()
            .any(|name| !matches!(name.as_str(), "key" | "normalize" | "ttl"))
        {
            return Err(invalid("only key, normalize, and ttl are supported"));
        }
        let paths = args
            .get("key")
            .and_then(serde_json::Value::as_array)
            .filter(|paths| !paths.is_empty())
            .ok_or_else(|| invalid("key must be a non-empty array"))?
            .iter()
            .map(|path| {
                let path = path
                    .as_str()
                    .ok_or_else(|| invalid("key values must be strings"))?;
                if path != "$request.url" && path.strip_prefix("$vals.").is_none_or(str::is_empty) {
                    return Err(invalid(format!("unsupported fingerprint path: {path}")));
                }
                Ok(path.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            paths,
            normalize: Normalize::parse(args.get("normalize"))?,
            ttl: Ttl::parse(args.get("ttl"))?,
        })
    }

    pub fn fingerprint(
        &self,
        request: &crate::net::Request,
    ) -> Result<Option<Fingerprint>, crate::middleware::Error> {
        if self.ttl == Ttl::Disabled {
            return Ok(None);
        }
        super::fingerprint::build(self, request).map(Some)
    }

    pub fn ttl(&self) -> Ttl {
        self.ttl
    }

    pub(super) fn paths(&self) -> &[String] {
        &self.paths
    }

    pub(super) fn normalize(&self) -> &Normalize {
        &self.normalize
    }
}

impl Ttl {
    fn parse(value: Option<&serde_json::Value>) -> Result<Self, crate::middleware::Error> {
        let Some(value) = value else {
            return Ok(Self::Permanent);
        };
        if value.as_i64() == Some(-1) {
            return Ok(Self::Permanent);
        }
        match value.as_u64() {
            Some(0) => Ok(Self::Disabled),
            Some(value) => Ok(Self::Finite(Duration::from_millis(value))),
            None => Err(invalid(
                "ttl must be -1 or non-negative integer milliseconds",
            )),
        }
    }
}

impl Normalize {
    fn parse(value: Option<&serde_json::Value>) -> Result<Self, crate::middleware::Error> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let object = value
            .as_object()
            .ok_or_else(|| invalid("normalize must be an object"))?;
        if object
            .keys()
            .any(|name| !matches!(name.as_str(), "enabled" | "drop_query" | "sort_query"))
        {
            return Err(invalid(
                "normalize only supports enabled, drop_query, and sort_query",
            ));
        }
        let enabled = boolean(object.get("enabled"), "normalize.enabled")?.unwrap_or(true);
        let sort_query = boolean(object.get("sort_query"), "normalize.sort_query")?.unwrap_or(true);
        let drop_query = match object.get("drop_query") {
            Some(value) => value
                .as_array()
                .ok_or_else(|| invalid("normalize.drop_query must be an array of strings"))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(ToOwned::to_owned)
                        .ok_or_else(|| invalid("normalize.drop_query must contain strings"))
                })
                .collect::<Result<HashSet<_>, _>>()?,
            None => HashSet::new(),
        };
        Ok(Self {
            enabled,
            drop_query,
            sort_query,
        })
    }

    pub(super) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(super) fn drop_query(&self) -> &HashSet<String> {
        &self.drop_query
    }

    pub(super) fn sort_query(&self) -> bool {
        self.sort_query
    }
}

impl Default for Normalize {
    fn default() -> Self {
        Self {
            enabled: true,
            drop_query: HashSet::new(),
            sort_query: true,
        }
    }
}

fn boolean(
    value: Option<&serde_json::Value>,
    name: &str,
) -> Result<Option<bool>, crate::middleware::Error> {
    value
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| invalid(format!("{name} must be boolean")))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(args: serde_json::Value) -> Spec {
        Spec::new("dedup").args(args)
    }

    #[test]
    fn parses_flat_configuration_and_ttl_boundaries() {
        let permanent = Config::from_spec(&spec(serde_json::json!({
            "key": ["$request.url"]
        })))
        .unwrap();
        assert_eq!(permanent.ttl(), Ttl::Permanent);

        let disabled = Config::from_spec(&spec(serde_json::json!({
            "key": ["$request.url"],
            "ttl": 0
        })))
        .unwrap();
        assert_eq!(disabled.ttl(), Ttl::Disabled);

        let finite = Config::from_spec(&spec(serde_json::json!({
            "key": ["$request.url"],
            "ttl": u64::MAX
        })))
        .unwrap();
        assert_eq!(finite.ttl(), Ttl::Finite(Duration::from_millis(u64::MAX)));
    }

    #[test]
    fn rejects_nested_rules_unknown_fields_and_invalid_paths() {
        for args in [
            serde_json::json!({"rules": {"url": {"key": ["$request.url"]}}}),
            serde_json::json!({"key": ["$request.url"], "namespace": "x"}),
            serde_json::json!({"key": []}),
            serde_json::json!({"key": ["$vals."]}),
        ] {
            assert!(Config::from_spec(&spec(args)).is_err());
        }
    }

    #[test]
    fn rejects_null_and_other_negative_ttl_values() {
        for ttl in [serde_json::Value::Null, serde_json::json!(-2)] {
            assert!(
                Config::from_spec(&spec(serde_json::json!({
                    "key": ["$request.url"],
                    "ttl": ttl
                })))
                .is_err()
            );
        }
    }

    #[test]
    fn validates_normalization_fields() {
        let config = Config::from_spec(&spec(serde_json::json!({
            "key": ["$request.url"],
            "normalize": {
                "enabled": false,
                "drop_query": ["utm_source"],
                "sort_query": false
            }
        })))
        .unwrap();
        assert!(!config.normalize().enabled());
        assert!(config.normalize().drop_query().contains("utm_source"));
        assert!(!config.normalize().sort_query());

        assert!(
            Config::from_spec(&spec(serde_json::json!({
                "key": ["$request.url"],
                "normalize": {"unknown": true}
            })))
            .is_err()
        );
    }
}
