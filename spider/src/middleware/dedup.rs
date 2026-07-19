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
        // Resolve every finite deadline before touching the store. If one value
        // cannot be represented by the runtime clock, this request must leave
        // the existing fingerprint set unchanged.
        let expirations = fingerprints
            .iter()
            .map(|(_, ttl)| {
                ttl.filter(|ttl| !ttl.is_zero())
                    .map(|ttl| {
                        now.checked_add(ttl).ok_or_else(|| {
                            invalid_config(
                                "ttl exceeds the runtime clock range; use -1 for permanent",
                            )
                        })
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut store = self.store();
        store.remove_expired(now);

        if fingerprints
            .iter()
            .any(|(fingerprint, _)| store.fingerprints.contains_key(fingerprint))
        {
            return Ok(true);
        }

        for ((fingerprint, ttl), expires) in fingerprints.into_iter().zip(expirations) {
            if ttl.is_some_and(|ttl| ttl.is_zero()) {
                continue;
            }
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
    if spec.skip {
        return Ok(());
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
            if key != "$request.url" && key.strip_prefix("$vals.").is_none_or(str::is_empty) {
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
        .filter_map(|(rule_name, rule)| match parse_ttl(rule) {
            Ok(Some(ttl)) if ttl.is_zero() => None,
            result => Some((rule_name, rule, result)),
        })
        .map(|(rule_name, rule, ttl)| {
            let ttl = ttl?;
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
            let digest = fingerprint(&(
                request.task_id.as_str(),
                spec.key.as_deref(),
                rule_name.as_str(),
                values,
            ))?;

            Ok((digest, ttl))
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
        query.sort_by(|(left, _), (right, _)| left.cmp(right));
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
    if ttl.as_i64() == Some(-1) {
        return Ok(None);
    }
    ttl.as_u64()
        .map(Duration::from_millis)
        .map(Some)
        .ok_or_else(|| invalid_config("ttl must be -1 or non-negative integer milliseconds"))
}

fn fingerprint(value: &impl serde::Serialize) -> Result<String, crate::middleware::Error> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| invalid_config(&format!("fingerprint cannot be serialized: {error}")))?;
    let mut hasher = Sha256::new();
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
    use std::sync::{Arc, Barrier};

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
        assert_eq!(
            parse_ttl(&serde_json::json!({"ttl": u64::MAX})).unwrap(),
            Some(Duration::from_millis(u64::MAX))
        );
    }

    #[test]
    fn null_and_other_negative_ttl_are_rejected() {
        for ttl in [serde_json::Value::Null, serde_json::json!(-2)] {
            let error = parse_ttl(&serde_json::json!({"ttl": ttl})).unwrap_err();
            assert!(error.to_string().contains("ttl must be -1"));
        }
    }

    #[test]
    fn rejects_empty_vals_path() {
        let spec = Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "value": {
                    "key": ["$vals."]
                }
            }
        }));

        let error = check(&spec).unwrap_err();

        assert!(error.to_string().contains("unsupported fingerprint path"));
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
    async fn keeps_duplicate_query_values_in_input_order() {
        let dedup = Dedup::default();
        let first = Request::follow("https://example.com/a?step=2&step=1").unwrap();
        let second = Request::follow("https://example.com/a?step=1&step=2").unwrap();

        assert!(matches!(
            dedup.before_scheduler(first, &spec()).await.unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            dedup.before_scheduler(second, &spec()).await.unwrap(),
            Next::Continue(_)
        ));
    }

    #[test]
    fn normalizes_query_by_key_without_reordering_duplicate_values() {
        let normalized = normalize_url(
            "https://example.com/a?step=2&b=2&a=1&step=1",
            Some(&serde_json::json!({"enabled": true})),
        )
        .unwrap();

        assert_eq!(normalized, "https://example.com/a?a=1&b=2&step=2&step=1");
    }

    #[tokio::test]
    async fn separates_dedup_instances_and_task_ids_without_delimiter_collisions() {
        let dedup = Dedup::default();

        let explicit_default = spec().key("default");
        let request = Request::follow("https://example.com/a").unwrap();
        assert!(matches!(
            dedup
                .before_scheduler(request.clone(), &spec())
                .await
                .unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            dedup
                .before_scheduler(request, &explicit_default)
                .await
                .unwrap(),
            Next::Continue(_)
        ));

        let first = spec().key("b:c");
        let second = spec().key("c");
        let mut first_request = Request::follow("https://example.com/b").unwrap();
        first_request.task_id = "a".to_string();
        let mut second_request = Request::follow("https://example.com/b").unwrap();
        second_request.task_id = "a:b".to_string();
        assert!(matches!(
            dedup.before_scheduler(first_request, &first).await.unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            dedup
                .before_scheduler(second_request, &second)
                .await
                .unwrap(),
            Next::Continue(_)
        ));
    }

    #[test]
    fn ttl_failure_does_not_partially_write_fingerprints() {
        let dedup = Dedup::default();
        if Instant::now().checked_add(Duration::MAX).is_some() {
            return;
        }

        let error = dedup
            .contains_or_insert(vec![
                ("first".to_string(), None),
                ("second".to_string(), Some(Duration::MAX)),
            ])
            .unwrap_err();

        assert!(error.to_string().contains("runtime clock range"));
        assert!(dedup.store().fingerprints.is_empty());
        assert!(dedup.store().expirations.is_empty());
    }

    #[test]
    fn concurrent_identical_fingerprints_only_one_inserts() {
        let dedup = Arc::new(Dedup::default());
        let barrier = Arc::new(Barrier::new(16));
        let mut tasks = Vec::new();

        for _ in 0..16 {
            let dedup = dedup.clone();
            let barrier = barrier.clone();
            tasks.push(std::thread::spawn(move || {
                barrier.wait();
                dedup
                    .contains_or_insert(vec![("same".to_string(), Some(Duration::from_secs(60)))])
                    .unwrap()
            }));
        }

        let mut existing = 0;
        let mut inserted = 0;
        for task in tasks {
            if task.join().unwrap() {
                existing += 1;
            } else {
                inserted += 1;
            }
        }
        assert_eq!(inserted, 1);
        assert_eq!(existing, 15);
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

    #[tokio::test]
    async fn zero_ttl_ignores_existing_fingerprint_without_disabling_other_rules() {
        let dedup = Dedup::default();
        let disabled = Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "disabled": {
                    "key": ["$request.url"]
                }
            }
        }));
        let mixed = Spec::new("dedup").args(serde_json::json!({
            "rules": {
                "disabled": {
                    "key": ["$request.url"],
                    "ttl": 0
                },
                "category": {
                    "key": ["$vals.category"]
                }
            }
        }));

        let first = Request::follow("https://example.com/a").unwrap();
        assert!(matches!(
            dedup.before_scheduler(first, &disabled).await.unwrap(),
            Next::Continue(_)
        ));

        let with_category = |category: &str| {
            Request::follow("https://example.com/a")
                .unwrap()
                .vals("category", category)
        };
        assert!(matches!(
            dedup
                .before_scheduler(with_category("books"), &mixed)
                .await
                .unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            dedup
                .before_scheduler(with_category("books"), &mixed)
                .await
                .unwrap(),
            Next::Skip
        ));
        assert!(matches!(
            dedup
                .before_scheduler(with_category("music"), &mixed)
                .await
                .unwrap(),
            Next::Continue(_)
        ));

        assert_eq!(dedup.store().fingerprints.len(), 3);
    }
}
