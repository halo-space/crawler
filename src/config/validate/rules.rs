use std::collections::HashSet;

use crate::{config::Error, graph};

pub(super) fn check(config: &graph::Config) -> Result<(), Error> {
    for (name, node) in &config.nodes {
        check_node(name, node)?;
    }

    for edge in &config.edges {
        let source = config
            .nodes
            .get(&edge.from)
            .ok_or_else(|| Error::Message(format!("edge source does not exist: {}", edge.from)))?;
        if let Some(each) = &edge.each {
            if edge.kind != graph::edge::Kind::Item {
                return Err(Error::Message(format!(
                    "request edge from {} must not define each",
                    edge.from
                )));
            }
            check_reference(
                &edge.from,
                each,
                &source.fields,
                &source.binds.keys().map(String::as_str).collect(),
            )?;
        }
        if let Some(request) = &edge.request {
            check_value(&edge.from, &request.url, source, false)?;
            for value_ref in request.vals.values() {
                check_value(&edge.from, value_ref, source, false)?;
            }
        }
        for value_ref in edge.vals.values() {
            check_value(&edge.from, value_ref, source, edge.each.is_some())?;
        }
        if let Some(when) = &edge.when {
            check_condition(
                &edge.from,
                when,
                source,
                &source.binds.keys().map(String::as_str).collect(),
            )?;
        }
    }

    Ok(())
}

fn check_condition(
    node: &str,
    condition: &graph::edge::Condition,
    source: &graph::node::Config,
    binds: &HashSet<&str>,
) -> Result<(), Error> {
    match condition {
        graph::edge::Condition::Predicate(predicate) => {
            check_reference(node, &predicate.from, &source.fields, binds)?;
            match predicate.op {
                graph::edge::ConditionOp::Present | graph::edge::ConditionOp::Absent => {
                    if predicate.value.is_some() {
                        return Err(Error::Message(format!(
                            "edge from {node} {:#?} does not accept value",
                            predicate.op
                        )));
                    }
                }
                _ => {
                    let value = predicate.value.as_ref().ok_or_else(|| {
                        Error::Message(format!("edge from {node} condition requires value"))
                    })?;
                    check_value(node, value, source, false)?;
                }
            }
        }
        graph::edge::Condition::All(group) => {
            if group.all.is_empty() {
                return Err(Error::Message(format!(
                    "edge from {node} when.all must not be empty"
                )));
            }
            for condition in &group.all {
                check_condition(node, condition, source, binds)?;
            }
        }
        graph::edge::Condition::Any(group) => {
            if group.any.is_empty() {
                return Err(Error::Message(format!(
                    "edge from {node} when.any must not be empty"
                )));
            }
            for condition in &group.any {
                check_condition(node, condition, source, binds)?;
            }
        }
    }
    Ok(())
}

fn check_node(name: &str, node: &graph::node::Config) -> Result<(), Error> {
    for (field_name, field) in &node.fields {
        if field.extractors.is_empty() && field.required {
            return Err(Error::Message(format!(
                "node {name} required field {field_name} has no extractors"
            )));
        }
        for extractor in &field.extractors {
            if extractor.expr().trim().is_empty() {
                return Err(Error::Message(format!(
                    "node {name} field {field_name} extractor expr is empty"
                )));
            }
            match extractor {
                graph::rules::Extractor::Css { expr, .. } => {
                    let (selector, _) = graph::rules::parse_css_output(expr).map_err(|error| {
                        Error::Message(format!(
                            "node {name} field {field_name} invalid css output: {error}"
                        ))
                    })?;
                    scrape_core::CompiledSelector::compile(selector).map_err(|error| {
                        Error::Message(format!(
                            "node {name} field {field_name} invalid css selector: {error}"
                        ))
                    })?;
                }
                graph::rules::Extractor::Regex { expr, .. } => {
                    regex::Regex::new(expr).map_err(|error| {
                        Error::Message(format!(
                            "node {name} field {field_name} invalid regex: {error}"
                        ))
                    })?;
                }
                graph::rules::Extractor::Json { expr } => {
                    crate::selector::json::compile(expr).map_err(|error| {
                        Error::Message(format!("node {name} field {field_name} {error}"))
                    })?;
                }
                graph::rules::Extractor::Ai { .. } => {}
            }
        }
    }

    let mut available_bind = HashSet::new();
    for (bind_name, bind) in &node.binds {
        match bind {
            graph::rules::Bind::Pipeline { from, transforms } => {
                check_reference(name, from, &node.fields, &available_bind)?;
                for step in transforms {
                    if !matches!(
                        step.kind.as_str(),
                        "trim"
                            | "normalize_space"
                            | "lowercase"
                            | "uppercase"
                            | "url_join"
                            | "url_normalize"
                            | "add"
                    ) {
                        return Err(Error::Message(format!(
                            "node {name} bind {bind_name} uses unsupported transform: {}",
                            step.kind
                        )));
                    }
                    check_transform(name, bind_name, step, &node.fields, &available_bind)?;
                }
            }
            graph::rules::Bind::Template { template, vars } => {
                if template.is_empty() {
                    return Err(Error::Message(format!(
                        "node {name} bind {bind_name} template is empty"
                    )));
                }
                let template_value = serde_json::Value::String(template.clone());
                let references = crate::net::request::template_references(&template_value)
                    .map_err(|error| {
                        Error::Message(format!(
                            "node {name} bind {bind_name} has an invalid template: {error}"
                        ))
                    })?;
                if let Some(reference) = references
                    .into_iter()
                    .find(|reference| !vars.contains_key(*reference))
                {
                    return Err(Error::Message(format!(
                        "node {name} bind {bind_name} template variable is undefined: {reference}"
                    )));
                }
                for value in vars.values() {
                    let from = value.from.as_deref().ok_or_else(|| {
                        Error::Message(format!(
                            "node {name} bind {bind_name} template variable requires from"
                        ))
                    })?;
                    check_reference(name, from, &node.fields, &available_bind)?;
                }
            }
        }
        available_bind.insert(bind_name.as_str());
    }
    Ok(())
}

fn check_transform(
    node: &str,
    bind_name: &str,
    transform: &graph::rules::Transform,
    fields: &graph::rules::Fields,
    available_bind: &HashSet<&str>,
) -> Result<(), Error> {
    match transform.kind.as_str() {
        "add" => {
            if transform.args.keys().any(|name| name != "value")
                || !transform.args.contains_key("value")
            {
                return Err(Error::Message(format!(
                    "node {node} bind {bind_name} add requires only value"
                )));
            }
            let value = transform.args.get("value").expect("checked above");
            if let Some(path) = value.as_str().filter(|value| value.starts_with('$')) {
                check_reference(node, path, fields, available_bind)?;
            } else if !value.is_i64() {
                return Err(Error::Message(format!(
                    "node {node} bind {bind_name} add.value must be an integer or reference"
                )));
            }
        }
        "url_join" => {
            if transform.args.keys().any(|name| name != "base_url") {
                return Err(Error::Message(format!(
                    "node {node} bind {bind_name} url_join only supports base_url"
                )));
            }
            if let Some(base_url) = transform.args.get("base_url") {
                let base_url = base_url.as_str().ok_or_else(|| {
                    Error::Message(format!(
                        "node {node} bind {bind_name} url_join.base_url must be a string"
                    ))
                })?;
                if base_url.starts_with('$') {
                    check_reference(node, base_url, fields, available_bind)?;
                } else {
                    let base = url::Url::parse(base_url).map_err(|error| {
                        Error::Message(format!(
                            "node {node} bind {bind_name} url_join.base_url is invalid: {error}"
                        ))
                    })?;
                    if base.cannot_be_a_base() {
                        return Err(Error::Message(format!(
                            "node {node} bind {bind_name} url_join.base_url cannot be used as a base URL"
                        )));
                    }
                }
            }
        }
        "trim" | "normalize_space" | "lowercase" | "uppercase" | "url_normalize"
            if !transform.args.is_empty() =>
        {
            return Err(Error::Message(format!(
                "node {node} bind {bind_name} transform {} does not accept arguments",
                transform.kind
            )));
        }
        _ => {}
    }
    Ok(())
}

fn check_reference<'a>(
    node: &str,
    path: &'a str,
    fields: &graph::rules::Fields,
    binds: &HashSet<&'a str>,
) -> Result<(), Error> {
    let Some((root, name)) = path.strip_prefix('$').and_then(|path| path.split_once('.')) else {
        return Err(Error::Message(format!(
            "node {node} contains invalid value reference: {path}"
        )));
    };
    let valid = match root {
        "fields" => fields.contains_key(name),
        "binds" => binds.contains(name),
        "vals" => !name.is_empty(),
        "response" => matches!(name, "url" | "status"),
        "request" => matches!(name, "url" | "node"),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Message(format!(
            "node {node} contains undefined value reference: {path}"
        )))
    }
}

fn check_value(
    node: &str,
    value: &graph::rules::ValueRef,
    source: &graph::node::Config,
    allow_item: bool,
) -> Result<(), Error> {
    match (&value.from, &value.value) {
        (Some(from), None) if allow_item && from.starts_with("$item.") => {
            if from.trim_start_matches("$item.").is_empty() {
                Err(Error::Message(format!(
                    "node {node} contains invalid item value reference: {from}"
                )))
            } else {
                Ok(())
            }
        }
        (Some(from), None) => check_reference(
            node,
            from,
            &source.fields,
            &source.binds.keys().map(String::as_str).collect(),
        ),
        (None, Some(_)) => Ok(()),
        (Some(_), Some(_)) => Err(Error::Message(format!(
            "node {node} value reference cannot define both from and value"
        ))),
        (None, None) => Err(Error::Message(format!(
            "node {node} value reference requires from or value"
        ))),
    }
}
