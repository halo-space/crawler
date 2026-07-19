use indexmap::IndexMap;
use serde_json::Value;

use super::value::{self, Context};
use crate::{config, graph, item, net};

/// 根据 request edge 构造下一组可调度 Request。
pub(super) fn requests(
    config: &config::Config,
    edge: &graph::edge::Spec,
    context: &Context<'_>,
) -> Result<Vec<net::Request>, crate::Error> {
    let spec = edge
        .request
        .as_ref()
        .ok_or_else(|| crate::Error::message("request edge requires request"))?;
    let target = spec.node.as_str();
    if !config.graph.nodes.contains_key(target) {
        return Err(crate::Error::message(format!(
            "request edge target does not exist: {target}"
        )));
    }
    let urls = value::resolve(&spec.url, context)?;
    let urls_are_array = urls.is_array();
    let urls = extract_urls(urls)?;
    if urls.is_empty() {
        return Ok(Vec::new());
    }

    let vals = spec
        .vals
        .iter()
        .map(|(name, value_ref)| value::resolve(value_ref, context).map(|value| (name, value)))
        .collect::<Result<Vec<_>, _>>()?;

    if !urls_are_array && let Some((name, _)) = vals.iter().find(|(_, value)| value.is_array()) {
        return Err(crate::Error::message(format!(
            "request edge val {name} is an array but url is scalar"
        )));
    }
    if let Some((name, actual)) = vals.iter().find_map(|(name, value)| {
        value
            .as_array()
            .filter(|value| value.len() != urls.len())
            .map(|value| ((*name).clone(), value.len()))
    }) {
        return Err(crate::Error::message(format!(
            "request edge val {name} has {actual} values but url has {}",
            urls.len()
        )));
    }

    urls.into_iter()
        .enumerate()
        .map(|(index, url)| {
            let mut request = context
                .response
                .follow(&url)
                .map_err(|error| crate::Error::message(error.to_string()))?;
            request.set_node(target);
            if let Some(snapshot) = context.request.snapshot().cloned() {
                request.set_snapshot(snapshot);
            }
            request.task_id.clone_from(&context.request.task_id);
            request.trace_id.clone_from(&context.request.trace_id);
            request.vals = context.request.vals.clone();
            for (name, value) in &vals {
                let value = value
                    .as_array()
                    .map_or_else(|| value.clone(), |values| values[index].clone());
                request.vals.insert((*name).clone(), value);
            }
            if urls_are_array {
                request
                    .vals
                    .insert("idx".to_string(), Value::from(index + 1));
            }
            request.priority = spec.priority.or(config.spider.priority).unwrap_or_default();
            let request_url = request.url.clone();
            let request_node_key = request.node_key().to_string();
            let request_vals = request.vals.clone();
            spec.transport
                .apply_with(&mut request, |name| match name {
                    "request.url" => Some(Value::String(request_url.clone())),
                    "request.node" => Some(Value::String(request_node_key.clone())),
                    "response.url" => Some(Value::String(context.response.url.clone())),
                    "response.status" => Some(Value::from(context.response.status.0)),
                    name if !name.contains('.') => context
                        .bind
                        .get(name)
                        .or_else(|| context.fields.get(name))
                        .cloned()
                        .or_else(|| request_vals.get(name).cloned()),
                    name => name
                        .strip_prefix("bind.")
                        .and_then(|name| context.bind.get(name).cloned())
                        .or_else(|| {
                            name.strip_prefix("fields.")
                                .and_then(|name| context.fields.get(name).cloned())
                        })
                        .or_else(|| {
                            name.strip_prefix("vals.")
                                .and_then(|name| request_vals.get(name).cloned())
                        }),
                })
                .map_err(crate::Error::Config)?;
            Ok(request)
        })
        .collect()
}

/// 根据 item edge 构造候选 Item，最终校验和提交仍走统一 Item 链。
pub(super) fn item(
    config: &item::Config,
    edge: &graph::edge::Spec,
    context: &Context<'_>,
) -> Result<item::Values, crate::Error> {
    let mut fields = IndexMap::new();
    let schema_fields = config.schema_fields().map_err(crate::Error::Item)?;
    for name in schema_fields.keys() {
        let mut value = context
            .bind
            .get(name)
            .cloned()
            .or_else(|| context.fields.get(name).cloned())
            .unwrap_or(Value::Null);
        if let Some(value_ref) = edge.vals.get(name) {
            let replacement = value::resolve(value_ref, context)?;
            if !value::is_empty(&replacement) {
                value = replacement;
            }
        }
        let value = crate::item::media::normalize(value, config.kind(name), &context.response.url);
        fields.insert(name.clone(), value);
    }
    Ok(fields)
}

pub(super) fn extract_urls(value: Value) -> Result<Vec<String>, crate::Error> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(vec![value]),
        Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                Value::String(value) if !value.is_empty() => Ok(value),
                _ => Err(crate::Error::message(
                    "request edge url array must contain only non-empty strings",
                )),
            })
            .collect(),
        Value::Null => Ok(Vec::new()),
        _ => Err(crate::Error::message(
            "request edge url must be a string or string array",
        )),
    }
}
