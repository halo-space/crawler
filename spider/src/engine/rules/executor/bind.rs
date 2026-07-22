use indexmap::IndexMap;
use serde_json::Value;
use std::collections::HashMap;

use super::value::{self, Context};
use crate::graph::rules::{Bind, Transform, ValueRef};
use crate::utils::template::{self, Part};
use crate::{graph, net};

/// 按声明顺序计算 bind，后一个 bind 可以引用前一个 bind。
pub(super) fn evaluate(
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
                let mut value = value::resolve_path(from, &context)?;
                for transform in transforms {
                    value = apply(value, transform, &context)?;
                }
                value
            }
            Bind::Template { template, vars } => render(template, vars, &context)?,
        };
        bind.insert(name.clone(), value);
    }
    Ok(bind)
}

fn apply(
    value: Value,
    transform: &Transform,
    context: &Context<'_>,
) -> Result<Value, crate::Error> {
    if let Value::Array(items) = value {
        return items
            .into_iter()
            .map(|item| apply(item, transform, context))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if value.is_null() {
        return Ok(Value::Null);
    }
    let text = value::scalar(&value)?;
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
            let base = value::scalar(&base)?;
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
    let parts = template::parse(template)
        .map_err(|error| crate::Error::message(format!("invalid template: {error}")))?;
    let mut values = HashMap::with_capacity(vars.len());
    for (name, value_ref) in vars {
        let value = value::resolve(value_ref, context)?;
        if value.is_null() {
            return Ok(Value::Null);
        }
        if value.is_array() || value.is_object() {
            return Err(crate::Error::message(format!(
                "template variable {name} must be scalar"
            )));
        }
        values.insert(name.as_str(), value::scalar(&value)?);
    }

    let mut rendered = String::with_capacity(template.len());
    for part in parts {
        match part {
            Part::Text(text) => rendered.push_str(text),
            Part::Variable(name) => rendered.push_str(values.get(name).ok_or_else(|| {
                crate::Error::message(format!("template contains undeclared variable: {name}"))
            })?),
        }
    }
    Ok(Value::String(rendered))
}

fn resolve_argument(value: &Value, context: &Context<'_>) -> Result<Value, crate::Error> {
    value
        .as_str()
        .filter(|value| value.starts_with('$'))
        .map(|path| value::resolve_path(path, context))
        .unwrap_or_else(|| Ok(value.clone()))
}
