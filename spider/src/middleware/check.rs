use super::{Error, Spec};

const HOOKS: &[&str] = &[
    "before_spider",
    "after_spider",
    "before_scheduler",
    "before_download",
    "after_download",
    "before_parse",
    "before_item",
    "error_download",
    "error_parse",
    "error_item",
];

/// Validates one middleware specification without executing a crawler task.
pub fn check(spec: &Spec) -> Result<(), Error> {
    if spec.name.trim().is_empty() {
        return Err(Error::InvalidConfig {
            name: spec.name.clone(),
            message: "name must not be empty".to_string(),
        });
    }
    if let Some(hook) = spec.hook.as_deref()
        && !HOOKS.contains(&hook)
    {
        return Err(invalid(spec, &format!("unsupported hook: {hook}")));
    }
    if !spec.skip && !spec.args.is_null() && !spec.args.is_object() {
        return Err(invalid(spec, "args must be an object"));
    }

    match spec.name.as_str() {
        "dedup" => super::dedup::check(spec),
        "rate_limit" => super::rate_limit::check(spec),
        "retry" => super::retry::check(spec),
        "validate" => super::validate::check(spec),
        _ => Ok(()),
    }
}

fn invalid(spec: &Spec, message: &str) -> Error {
    Error::InvalidConfig {
        name: spec.name.clone(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_builtin_specs_for_api_callers() {
        check(&Spec::new("dedup").args(serde_json::json!({
            "key": ["$request.url"],
            "ttl": -1
        })))
        .unwrap();
        check(&Spec::new("rate_limit").args(serde_json::json!({"qps": 2.5}))).unwrap();
        check(&Spec::new("retry").args(serde_json::json!({
            "count": 2,
            "backoff": [100, 200]
        })))
        .unwrap();
    }

    #[test]
    fn rejects_invalid_builtin_specs() {
        assert!(check(&Spec::new("retry").args(serde_json::json!({"count": "2"}))).is_err());
        assert!(check(&Spec::new("rate_limit").args(serde_json::json!({"qps": 0}))).is_err());
        assert!(check(&Spec::new("dedup").args(serde_json::json!({"key": []}))).is_err());
    }

    #[test]
    fn skip_spec_does_not_require_middleware_args() {
        let mut spec = Spec::new("rate_limit");
        spec.skip = true;
        spec.args = serde_json::json!("ignored");

        check(&spec).unwrap();
    }

    #[test]
    fn skip_spec_still_requires_a_supported_builtin_hook() {
        for mut spec in [
            Spec::new("validate").hook("error_parse"),
            Spec::new("dedup").hook("before_download"),
            Spec::new("rate_limit").hook("before_scheduler"),
        ] {
            spec.skip = true;
            let error = check(&spec).unwrap_err();
            assert!(error.to_string().contains("hook must be"));
        }
    }
}
