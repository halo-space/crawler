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
    for (name, spec) in &node.binds {
        let context = Context {
            request,
            response,
            fields,
            binds: &bind,
            item: None,
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
    if transform.kind == "add" {
        let argument = transform
            .args
            .get("value")
            .ok_or_else(|| crate::Error::message("add requires value"))?;
        let argument = resolve_argument(argument, context)?;
        let left = value
            .as_i64()
            .ok_or_else(|| crate::Error::message("add input must be an integer"))?;
        let right = argument
            .as_i64()
            .ok_or_else(|| crate::Error::message("add value must be an integer"))?;
        let result = left
            .checked_add(right)
            .ok_or_else(|| crate::Error::message("add result overflows an integer"))?;
        return Ok(Value::from(result));
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
        if value.is_object() {
            return Err(crate::Error::message(format!(
                "template variable {name} must be scalar or array"
            )));
        }
        values.insert(name.as_str(), value);
    }

    let array_len = values.values().find_map(Value::as_array).map(Vec::len);
    if let Some(array_len) = array_len {
        for (name, value) in &values {
            if let Some(items) = value.as_array()
                && items.len() != array_len
            {
                return Err(crate::Error::message(format!(
                    "template variable {name} has {} values but another variable has {array_len}",
                    items.len()
                )));
            }
        }

        let rendered = (0..array_len)
            .map(|index| {
                let scalars = values
                    .iter()
                    .map(|(name, value)| {
                        let value = value.as_array().map(|items| &items[index]).unwrap_or(value);
                        Ok((*name, value::scalar(value)?))
                    })
                    .collect::<Result<HashMap<_, _>, crate::Error>>()?;
                render_parts(template, &parts, &scalars)
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Value::Array(
            rendered.into_iter().map(Value::String).collect(),
        ));
    }

    let scalars = values
        .iter()
        .map(|(name, value)| Ok((*name, value::scalar(value)?)))
        .collect::<Result<HashMap<_, _>, crate::Error>>()?;
    Ok(Value::String(render_parts(template, &parts, &scalars)?))
}

fn render_parts(
    template: &str,
    parts: &[Part<'_>],
    values: &HashMap<&str, String>,
) -> Result<String, crate::Error> {
    let mut rendered = String::with_capacity(template.len());
    for part in parts {
        match part {
            Part::Text(text) => rendered.push_str(text),
            Part::Variable(name) => rendered.push_str(values.get(name).ok_or_else(|| {
                crate::Error::message(format!("template contains undeclared variable: {name}"))
            })?),
        }
    }
    Ok(rendered)
}

fn resolve_argument(value: &Value, context: &Context<'_>) -> Result<Value, crate::Error> {
    value
        .as_str()
        .filter(|value| value.starts_with('$'))
        .map(|path| value::resolve_path(path, context))
        .unwrap_or_else(|| Ok(value.clone()))
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::*;

    #[test]
    fn add_supports_literal_and_reference_integers() {
        let request = net::Request::follow("https://example.com").unwrap();
        let response = net::Response::new(request.clone(), net::StatusCode(200), String::new());
        let fields = IndexMap::new();
        let binds = IndexMap::from([(String::from("step"), json!(20))]);
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            binds: &binds,
            item: None,
        };

        let literal = Transform {
            kind: "add".to_string(),
            args: Map::from_iter([(String::from("value"), json!(1))]),
        };
        assert_eq!(apply(json!(1), &literal, &context).unwrap(), json!(2));

        let reference = Transform {
            kind: "add".to_string(),
            args: Map::from_iter([(String::from("value"), json!("$binds.step"))]),
        };
        assert_eq!(apply(json!(2), &reference, &context).unwrap(), json!(22));
    }
}
