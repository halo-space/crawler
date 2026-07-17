use indexmap::IndexMap;
use serde_json::Value;

use crate::graph::rules::{Bind, Transform, ValueRef};
use crate::{graph, net};

pub(super) struct Context<'a> {
    pub(super) request: &'a net::Request,
    pub(super) response: &'a net::Response,
    pub(super) fields: &'a IndexMap<String, Value>,
    pub(super) bind: &'a IndexMap<String, Value>,
}

/// 按声明顺序计算 bind，后一个 bind 可以引用前一个 bind。
pub(super) fn bind(
    node: &graph::node::Config,
    request: &net::Request,
    response: &net::Response,
    fields: &IndexMap<String, Value>,
) -> Result<IndexMap<String, Value>, crate::Error> {
    let mut bind = IndexMap::new();
    for (name, spec) in &node.bind {
        let context = Context {
            request,
            response,
            fields,
            bind: &bind,
        };
        let value = match spec {
            Bind::Pipeline { from, transforms } => {
                let mut value = resolve_path(from, &context)?;
                for transform in transforms {
                    value = apply_transform(value, transform, &context)?;
                }
                value
            }
            Bind::Template { template, vars } => render(template, vars, &context)?,
        };
        bind.insert(name.clone(), value);
    }
    Ok(bind)
}

fn apply_transform(
    value: Value,
    transform: &Transform,
    context: &Context<'_>,
) -> Result<Value, crate::Error> {
    if let Value::Array(items) = value {
        return items
            .into_iter()
            .map(|item| apply_transform(item, transform, context))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if value.is_null() {
        return Ok(Value::Null);
    }
    let text = scalar(&value)?;
    match transform.kind.as_str() {
        "trim" => Ok(Value::String(text.trim().to_string())),
        "normalize_space" => Ok(Value::String(
            text.split_whitespace().collect::<Vec<_>>().join(" "),
        )),
        "lowercase" => Ok(Value::String(text.to_lowercase())),
        "uppercase" => Ok(Value::String(text.to_uppercase())),
        "url_join" => {
            let base = transform
                .args
                .get("base_url")
                .map(|value| resolve_argument(value, context))
                .transpose()?
                .unwrap_or_else(|| Value::String(context.response.url.clone()));
            let base = scalar(&base)?;
            let base =
                url::Url::parse(&base).map_err(|error| crate::Error::message(error.to_string()))?;
            Ok(Value::String(
                base.join(&text)
                    .map_err(|error| crate::Error::message(error.to_string()))?
                    .to_string(),
            ))
        }
        "url_normalize" => Ok(Value::String(
            url::Url::parse(&text)
                .map_err(|error| crate::Error::message(error.to_string()))?
                .to_string(),
        )),
        kind => Err(crate::Error::message(format!(
            "unsupported transform: {kind}"
        ))),
    }
}

pub(super) fn render(
    template: &str,
    vars: &IndexMap<String, ValueRef>,
    context: &Context<'_>,
) -> Result<Value, crate::Error> {
    let mut rendered = template.to_string();
    for (name, value_ref) in vars {
        let value = resolve(value_ref, context)?;
        if value.is_null() {
            return Ok(Value::Null);
        }
        if value.is_array() || value.is_object() {
            return Err(crate::Error::message(format!(
                "template variable {name} must be scalar"
            )));
        }
        rendered = rendered.replace(&format!("{{{name}}}"), &scalar(&value)?);
    }
    if rendered.contains('{') || rendered.contains('}') {
        return Err(crate::Error::message(
            "template contains undeclared variables",
        ));
    }
    Ok(Value::String(rendered))
}

pub(super) fn matches(when: Option<&str>, context: &Context<'_>) -> Result<bool, crate::Error> {
    let Some(when) = when else {
        return Ok(true);
    };
    if let Some(path) = when.strip_suffix(" != null") {
        return Ok(!is_empty(&resolve_path(path.trim(), context)?));
    }
    if let Some(path) = when.strip_suffix(" == null") {
        return Ok(is_empty(&resolve_path(path.trim(), context)?));
    }
    Err(crate::Error::message(format!(
        "unsupported edge.when expression: {when}"
    )))
}

pub(super) fn resolve(value_ref: &ValueRef, context: &Context<'_>) -> Result<Value, crate::Error> {
    match (&value_ref.from, &value_ref.value) {
        (Some(from), None) => resolve_path(from, context),
        (None, Some(value)) => Ok(value.clone()),
        (Some(_), Some(_)) => Err(crate::Error::message(
            "value reference cannot define both from and value",
        )),
        (None, None) => Err(crate::Error::message(
            "value reference requires from or value",
        )),
    }
}

fn resolve_argument(value: &Value, context: &Context<'_>) -> Result<Value, crate::Error> {
    value
        .as_str()
        .filter(|value| value.starts_with('$'))
        .map(|path| resolve_path(path, context))
        .unwrap_or_else(|| Ok(value.clone()))
}

fn resolve_path(path: &str, context: &Context<'_>) -> Result<Value, crate::Error> {
    let (root, name) = path
        .strip_prefix('$')
        .and_then(|path| path.split_once('.'))
        .ok_or_else(|| crate::Error::message(format!("invalid value reference: {path}")))?;
    match root {
        "fields" => context.fields.get(name).cloned(),
        "bind" => context.bind.get(name).cloned(),
        "vals" => context.request.vals.get(name).cloned(),
        "response" => match name {
            "url" => return Ok(Value::String(context.response.url.clone())),
            "status" => return Ok(Value::from(context.response.status.0)),
            _ => None,
        },
        "request" => match name {
            "url" => return Ok(Value::String(context.request.url.clone())),
            "node" => return Ok(Value::String(context.request.node_key().to_string())),
            _ => None,
        },
        _ => None,
    }
    .ok_or_else(|| crate::Error::message(format!("undefined value reference: {path}")))
}

fn scalar(value: &Value) -> Result<String, crate::Error> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => {
            Err(crate::Error::message("value must be scalar"))
        }
    }
}

pub(super) fn is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.is_empty(),
        Value::Array(value) => value.is_empty(),
        Value::Object(value) => value.is_empty(),
        Value::Bool(_) | Value::Number(_) => false,
    }
}
