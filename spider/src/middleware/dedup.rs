use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::Request;

#[derive(Default)]
pub struct Dedup {
    store: Mutex<Store>,
}

#[derive(Default)]
struct Store {
    fingerprints: HashMap<String, Option<Instant>>,
    expirations: BinaryHeap<Reverse<(Instant, String)>>,
}

impl Middleware for Dedup {
    fn order(&self, _hook: &str) -> i32 {
        400
    }

    fn before_scheduler<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            if request.dont_filter {
                return Ok(Next::Continue(request));
            }

            let fingerprints = request_fingerprints(&request, spec)?;
            if self.contains_or_insert(fingerprints)? {
                Ok(Next::Skip)
            } else {
                Ok(Next::Continue(request))
            }
        })
    }
}

impl Dedup {
    fn contains_or_insert(
        &self,
        fingerprints: Vec<(String, Option<Duration>)>,
    ) -> Result<bool, crate::middleware::Error> {
        let now = Instant::now();
        let mut store = self.store();
        store.remove_expired(now);

        if fingerprints
            .iter()
            .any(|(fingerprint, _)| store.fingerprints.contains_key(fingerprint))
        {
            return Ok(true);
        }

        for (fingerprint, ttl) in fingerprints {
            let expires = match ttl {
                Some(ttl) if ttl.is_zero() => continue,
                Some(ttl) => Some(now.checked_add(ttl).ok_or_else(|| {
                    invalid_config("ttl exceeds the runtime clock range; use -1 for permanent")
                })?),
                None => None,
            };
            store.fingerprints.insert(fingerprint.clone(), expires);
            if let Some(expires) = expires {
                store.expirations.push(Reverse((expires, fingerprint)));
            }
        }
        Ok(false)
    }

    fn store(&self) -> MutexGuard<'_, Store> {
        self.store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Store {
    fn remove_expired(&mut self, now: Instant) {
        while self
            .expirations
            .peek()
            .is_some_and(|Reverse((expires, _))| *expires <= now)
        {
            let Some(Reverse((expires, fingerprint))) = self.expirations.pop() else {
                break;
            };
            if self.fingerprints.get(&fingerprint) == Some(&Some(expires)) {
                self.fingerprints.remove(&fingerprint);
            }
        }
    }
}

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| hook != "before_scheduler")
    {
        return Err(invalid_config("hook must be before_scheduler"));
    }
    let rules = spec
        .args
        .get("rules")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| invalid_config("rules must be a non-empty object"))?;
    if spec
        .args
        .as_object()
        .is_some_and(|args| args.keys().any(|name| name != "rules"))
    {
        return Err(invalid_config("only rules is supported"));
    }
    if rules.is_empty() {
        return Err(invalid_config("rules must be a non-empty object"));
    }
    for rule in rules.values() {
        let rule = rule
            .as_object()
            .ok_or_else(|| invalid_config("each rule must be an object"))?;
        if rule
            .keys()
            .any(|name| !matches!(name.as_str(), "key" | "normalize" | "ttl"))
        {
            return Err(invalid_config("rule only supports key, normalize, and ttl"));
        }
        let keys = rule
            .get("key")
            .and_then(serde_json::Value::as_array)
            .filter(|keys| !keys.is_empty())
            .ok_or_else(|| invalid_config("rule key must be a non-empty array"))?;
        for key in keys {
            let key = key
                .as_str()
                .ok_or_else(|| invalid_config("rule key values must be strings"))?;
            if key != "$request.url" && !key.starts_with("$vals.") {
                return Err(invalid_config(&format!(
                    "unsupported fingerprint path: {key}"
                )));
            }
        }
        check_normalization(rule.get("normalize"))?;
        parse_ttl(&serde_json::Value::Object(rule.clone()))?;
    }
    Ok(())
}

fn check_normalization(value: Option<&serde_json::Value>) -> Result<(), crate::middleware::Error> {
    let Some(value) = value else {
        return Ok(());
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid_config("normalize must be an object"))?;
    if object
        .keys()
        .any(|name| !matches!(name.as_str(), "enabled" | "drop_query" | "sort_query"))
    {
        return Err(invalid_config(
            "normalize only supports enabled, drop_query, and sort_query",
        ));
    }
    for name in ["enabled", "sort_query"] {
        if let Some(value) = object.get(name)
            && !value.is_boolean()
        {
            return Err(invalid_config(&format!("normalize.{name} must be boolean")));
        }
    }
    if let Some(values) = object.get("drop_query") {
        let values = values
            .as_array()
            .ok_or_else(|| invalid_config("drop_query must be an array of strings"))?;
        if values.iter().any(|value| !value.is_string()) {
            return Err(invalid_config("drop_query must contain strings"));
        }
    }
    Ok(())
}

fn request_fingerprints(
    request: &Request,
    spec: &Spec,
) -> Result<Vec<(String, Option<Duration>)>, crate::middleware::Error> {
    let rules = spec
        .args
        .get("rules")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| invalid_config("rules must be a non-empty object"))?;
    if rules.is_empty() {
        return Err(invalid_config("rules must be a non-empty object"));
    }

    rules
        .iter()
        .map(|(rule_name, rule)| {
            let paths = rule
                .get("key")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| invalid_config("rule key must be a non-empty array"))?;
            if paths.is_empty() {
                return Err(invalid_config("rule key must be a non-empty array"));
            }

            let values = paths
                .iter()
                .map(|path| {
                    let path = path
                        .as_str()
                        .ok_or_else(|| invalid_config("rule key values must be strings"))?;
                    request_value(request, path, rule.get("normalize"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let ttl = parse_ttl(rule)?;
            let namespace = format!(
                "{}:{}:{}",
                request.task_id,
                spec.key.as_deref().unwrap_or("default"),
                rule_name
            );

            Ok((fingerprint(&namespace, &values)?, ttl))
        })
        .collect()
}

fn request_value(
    request: &Request,
    path: &str,
    normalization: Option<&serde_json::Value>,
) -> Result<serde_json::Value, crate::middleware::Error> {
    if path == "$request.url" {
        let enabled = normalization
            .and_then(|value| value.get("enabled"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let value = if enabled {
            normalize_url(&request.url, normalization)?
        } else {
            request.url.clone()
        };
        return Ok(serde_json::Value::String(value));
    }

    let Some(name) = path.strip_prefix("$vals.") else {
        return Err(invalid_config(&format!(
            "unsupported fingerprint path: {path}"
        )));
    };
    request
        .vals
        .get(name)
        .cloned()
        .ok_or_else(|| invalid_config(&format!("fingerprint path has no value: {path}")))
}

fn normalize_url(
    value: &str,
    normalization: Option<&serde_json::Value>,
) -> Result<String, crate::middleware::Error> {
    let mut url = url::Url::parse(value)
        .map_err(|error| invalid_config(&format!("URL cannot be normalized: {error}")))?;
    url.set_fragment(None);

    let drop_query = normalization
        .and_then(|value| value.get("drop_query"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| invalid_config("drop_query must contain strings"))
        })
        .collect::<Result<std::collections::HashSet<_>, _>>()?;
    let mut query = url
        .query_pairs()
        .into_owned()
        .filter(|(key, _)| !drop_query.contains(key))
        .collect::<Vec<_>>();
    if normalization
        .and_then(|value| value.get("sort_query"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true)
    {
        query.sort();
    }
    if query.is_empty() {
        url.set_query(None);
    } else {
        url.query_pairs_mut().clear().extend_pairs(query);
    }

    Ok(url.to_string())
}

fn parse_ttl(args: &serde_json::Value) -> Result<Option<Duration>, crate::middleware::Error> {
    let Some(ttl) = args.get("ttl") else {
        return Ok(None);
    };
    let ttl = ttl
        .as_i64()
        .ok_or_else(|| invalid_config("ttl must be -1 or non-negative integer milliseconds"))?;
    match ttl {
        -1 => Ok(None),
        0.. => Ok(Some(Duration::from_millis(ttl as u64))),
        _ => Err(invalid_config(
            "ttl must be -1 or non-negative integer milliseconds",
        )),
    }
}

fn fingerprint(
    namespace: &str,
    value: &impl serde::Serialize,
) -> Result<String, crate::middleware::Error> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| invalid_config(&format!("fingerprint cannot be serialized: {error}")))?;
    let mut hasher = Sha256::new();
    hasher.update(namespace.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn invalid_config(message: &str) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "dedup".to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> Spec {
        Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "url": {
                    "key": ["$request.url"],
                    "normalize": {"enabled": true},
                    "ttl": 60_000
                }
            }
        }))
    }

    #[test]
    fn missing_or_negative_one_ttl_never_expires() {
        assert_eq!(parse_ttl(&serde_json::json!({})).unwrap(), None);
        assert_eq!(parse_ttl(&serde_json::json!({"ttl": -1})).unwrap(), None);
    }

    #[test]
    fn non_negative_ttl_uses_milliseconds() {
        assert_eq!(
            parse_ttl(&serde_json::json!({"ttl": 2500})).unwrap(),
            Some(Duration::from_millis(2500))
        );
    }

    #[test]
    fn null_and_other_negative_ttl_are_rejected() {
        for ttl in [serde_json::Value::Null, serde_json::json!(-2)] {
            let error = parse_ttl(&serde_json::json!({"ttl": ttl})).unwrap_err();
            assert!(error.to_string().contains("ttl must be -1"));
        }
    }

    #[tokio::test]
    async fn skips_equivalent_normalized_urls() {
        let dedup = Dedup::default();
        let first = Request::follow("https://EXAMPLE.com/a?b=2&a=1#top").unwrap();
        let second = Request::follow("https://example.com/a?a=1&b=2").unwrap();

        assert!(matches!(
            dedup.before_scheduler(first, &spec()).await.unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            dedup.before_scheduler(second, &spec()).await.unwrap(),
            Next::Skip
        ));
    }

    #[tokio::test]
    async fn dont_filter_bypasses_fingerprint_store() {
        let dedup = Dedup::default();
        let mut first = Request::follow("https://example.com").unwrap();
        first.dont_filter = true;
        let second = Request::follow("https://example.com").unwrap();

        dedup.before_scheduler(first, &spec()).await.unwrap();
        assert!(matches!(
            dedup.before_scheduler(second, &spec()).await.unwrap(),
            Next::Continue(_)
        ));
    }

    #[tokio::test]
    async fn explicit_drop_query_is_part_of_normalization() {
        let dedup = Dedup::default();
        let spec = Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "url": {
                    "key": ["$request.url"],
                    "normalize": {
                        "enabled": true,
                        "drop_query": ["utm_source"]
                    }
                }
            }
        }));
        let first = Request::follow("https://example.com/a?id=1&utm_source=first").unwrap();
        let second = Request::follow("https://example.com/a?utm_source=second&id=1").unwrap();

        dedup.before_scheduler(first, &spec).await.unwrap();
        assert!(matches!(
            dedup.before_scheduler(second, &spec).await.unwrap(),
            Next::Skip
        ));
    }

    #[tokio::test]
    async fn zero_ttl_does_not_retain_the_fingerprint() {
        let dedup = Dedup::default();
        let spec = Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "url": {
                    "key": ["$request.url"],
                    "ttl": 0
                }
            }
        }));

        for _ in 0..2 {
            let request = Request::follow("https://example.com/a").unwrap();
            assert!(matches!(
                dedup.before_scheduler(request, &spec).await.unwrap(),
                Next::Continue(_)
            ));
        }
        assert!(dedup.store().fingerprints.is_empty());
    }
}
