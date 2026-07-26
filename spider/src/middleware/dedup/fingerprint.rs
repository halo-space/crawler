use sha2::{Digest as _, Sha256};

use super::{Config, invalid};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Fingerprint {
    task_id: String,
    node: String,
    value: String,
}

impl Fingerprint {
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

pub(super) fn build(
    config: &Config,
    request: &crate::net::Request,
) -> Result<Fingerprint, crate::middleware::Error> {
    if request.task_id.is_empty() {
        return Err(invalid("request task_id must not be empty"));
    }
    if request.node_key().is_empty() {
        return Err(invalid("request node must not be empty"));
    }
    let mut values = config
        .paths()
        .iter()
        .map(|path| value(config, request, path))
        .collect::<Result<Vec<_>, _>>()?;
    for value in &mut values {
        canonicalize(value);
    }
    let bytes = serde_json::to_vec(&values)
        .map_err(|error| invalid(format!("fingerprint cannot be serialized: {error}")))?;
    let mut hash = Sha256::new();
    hash.update(bytes);
    Ok(Fingerprint {
        task_id: request.task_id.clone(),
        node: request.node_key().to_string(),
        value: format!("{:x}", hash.finalize()),
    })
}

fn value(
    config: &Config,
    request: &crate::net::Request,
    path: &str,
) -> Result<serde_json::Value, crate::middleware::Error> {
    if path == "$request.url" {
        let value = if config.normalize().enabled() {
            normalize_url(&request.url, config)?
        } else {
            request.url.clone()
        };
        return Ok(serde_json::Value::String(value));
    }
    let name = path
        .strip_prefix("$vals.")
        .expect("Config validates every fingerprint path");
    request
        .vals
        .get(name)
        .cloned()
        .ok_or_else(|| invalid(format!("fingerprint path has no value: {path}")))
}

fn normalize_url(value: &str, config: &Config) -> Result<String, crate::middleware::Error> {
    let mut url = url::Url::parse(value)
        .map_err(|error| invalid(format!("URL cannot be normalized: {error}")))?;
    url.set_fragment(None);
    let mut query = url
        .query_pairs()
        .into_owned()
        .filter(|(key, _)| !config.normalize().drop_query().contains(key))
        .collect::<Vec<_>>();
    if config.normalize().sort_query() {
        query.sort_by(|(left, _), (right, _)| left.cmp(right));
    }
    if query.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(query);
    }
    Ok(url.to_string())
}

fn canonicalize(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                canonicalize(value);
            }
        }
        serde_json::Value::Object(values) => {
            let mut entries = std::mem::take(values).into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (_, value) in &mut entries {
                canonicalize(value);
            }
            values.extend(entries);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::Spec;

    fn config(paths: &[&str]) -> Config {
        Config::from_spec(&Spec::new("dedup").args(serde_json::json!({
            "key": paths
        })))
        .unwrap()
    }

    fn config_with(args: serde_json::Value) -> Config {
        Config::from_spec(&Spec::new("dedup").args(args)).unwrap()
    }

    fn request() -> crate::net::Request {
        let mut request =
            crate::net::Request::follow("https://EXAMPLE.com/article?b=2&a=1#section")
                .unwrap()
                .node("detail");
        request.task_id = "task-1".to_string();
        request
    }

    #[test]
    fn normalizes_urls_and_excludes_trace_or_middleware_identity() {
        let mut first = request();
        first.trace_id = "trace-a".to_string();
        let mut second = request();
        second.url = "https://example.com/article?a=1&b=2".to_string();
        second.trace_id = "trace-b".to_string();

        assert_eq!(
            config(&["$request.url"]).fingerprint(&first).unwrap(),
            config(&["$request.url"]).fingerprint(&second).unwrap()
        );
    }

    #[test]
    fn normalization_drops_configured_query_keys_and_preserves_duplicate_order() {
        let mut noisy = request();
        noisy.url =
            "https://example.com/article?utm_source=x&b=2&a=first&a=second#part".to_string();
        let mut normalized = request();
        normalized.url = "https://example.com/article?a=first&a=second&b=2".to_string();
        let mut reordered = request();
        reordered.url = "https://example.com/article?a=second&a=first&b=2".to_string();
        let dropping = config_with(serde_json::json!({
            "key": ["$request.url"],
            "normalize": {"drop_query": ["utm_source"]}
        }));

        assert_eq!(
            dropping.fingerprint(&noisy).unwrap(),
            config(&["$request.url"]).fingerprint(&normalized).unwrap()
        );
        assert_ne!(
            dropping.fingerprint(&noisy).unwrap(),
            config(&["$request.url"]).fingerprint(&reordered).unwrap()
        );

        let disabled = config_with(serde_json::json!({
            "key": ["$request.url"],
            "normalize": {"enabled": false}
        }));
        assert_ne!(
            disabled.fingerprint(&noisy).unwrap(),
            disabled.fingerprint(&normalized).unwrap()
        );
    }

    #[test]
    fn recursively_canonicalizes_objects_but_preserves_path_order_and_types() {
        let mut first = request();
        first.vals.insert(
            "value".to_string(),
            serde_json::json!({"b": 2, "a": {"d": 4, "c": 3}}),
        );
        first
            .vals
            .insert("other".to_string(), serde_json::json!("2"));
        let mut second = request();
        second.vals.insert(
            "value".to_string(),
            serde_json::json!({"a": {"c": 3, "d": 4}, "b": 2}),
        );
        second
            .vals
            .insert("other".to_string(), serde_json::json!("2"));

        assert_eq!(
            config(&["$vals.value", "$vals.other"])
                .fingerprint(&first)
                .unwrap(),
            config(&["$vals.value", "$vals.other"])
                .fingerprint(&second)
                .unwrap()
        );
        assert_ne!(
            config(&["$vals.value", "$vals.other"])
                .fingerprint(&first)
                .unwrap(),
            config(&["$vals.other", "$vals.value"])
                .fingerprint(&first)
                .unwrap()
        );
    }
}
