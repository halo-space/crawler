use indexmap::IndexMap;
use serde_json::Value;

use crate::graph::rules::ValueRef;
use crate::net;

pub(super) struct Context<'a> {
    pub(super) request: &'a net::Request,
    pub(super) response: &'a net::Response,
    pub(super) fields: &'a IndexMap<String, Value>,
    pub(super) bind: &'a IndexMap<String, Value>,
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

pub(super) fn resolve_path(path: &str, context: &Context<'_>) -> Result<Value, crate::Error> {
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

pub(super) fn scalar(value: &Value) -> Result<String, crate::Error> {
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
