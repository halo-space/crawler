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
        if let Some(request) = &edge.request {
            check_value(&edge.from, &request.url, source)?;
            for value_ref in request.vals.values() {
                check_value(&edge.from, value_ref, source)?;
            }
        }
        for value_ref in edge.vals.values() {
            check_value(&edge.from, value_ref, source)?;
        }
        if let Some(when) = &edge.when {
            let path = when
                .strip_suffix(" != null")
                .or_else(|| when.strip_suffix(" == null"))
                .ok_or_else(|| {
                    Error::Message(format!(
                        "edge from {} uses unsupported when expression: {when}",
                        edge.from
                    ))
                })?;
            check_reference(
                &edge.from,
                path.trim(),
                &source.parse,
                &source.bind.keys().map(String::as_str).collect(),
            )?;
        }
    }

    Ok(())
}

fn check_node(name: &str, node: &graph::node::Config) -> Result<(), Error> {
    for (field_name, field) in &node.parse.fields {
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
                    graph::rules::parse_css_output(expr).map_err(|error| {
                        Error::Message(format!(
                            "node {name} field {field_name} invalid css output: {error}"
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
                graph::rules::Extractor::Ai { .. } => {}
            }
        }
    }

    let mut available_bind = HashSet::new();
    for (bind_name, bind) in &node.bind {
        match bind {
            graph::rules::Bind::Pipeline { from, transforms } => {
                check_reference(name, from, &node.parse, &available_bind)?;
                for step in transforms {
                    if !matches!(
                        step.kind.as_str(),
                        "trim"
                            | "normalize_space"
                            | "lowercase"
                            | "uppercase"
                            | "url_join"
                            | "url_normalize"
                    ) {
                        return Err(Error::Message(format!(
                            "node {name} bind {bind_name} uses unsupported transform: {}",
                            step.kind
                        )));
                    }
                    check_transform(name, bind_name, step, &node.parse, &available_bind)?;
                }
            }
            graph::rules::Bind::Template { template, vars } => {
                if template.is_empty() {
                    return Err(Error::Message(format!(
                        "node {name} bind {bind_name} template is empty"
                    )));
                }
                for value in vars.values() {
                    let from = value.from.as_deref().ok_or_else(|| {
                        Error::Message(format!(
                            "node {name} bind {bind_name} template variable requires from"
                        ))
                    })?;
                    check_reference(name, from, &node.parse, &available_bind)?;
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
    parse: &graph::rules::Parse,
    available_bind: &HashSet<&str>,
) -> Result<(), Error> {
    match transform.kind.as_str() {
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
                    check_reference(node, base_url, parse, available_bind)?;
                } else {
                    url::Url::parse(base_url).map_err(|error| {
                        Error::Message(format!(
                            "node {node} bind {bind_name} url_join.base_url is invalid: {error}"
                        ))
                    })?;
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
    parse: &graph::rules::Parse,
    bind: &HashSet<&'a str>,
) -> Result<(), Error> {
    let Some((root, name)) = path.strip_prefix('$').and_then(|path| path.split_once('.')) else {
        return Err(Error::Message(format!(
            "node {node} contains invalid value reference: {path}"
        )));
    };
    let valid = match root {
        "fields" => parse.fields.contains_key(name),
        "bind" => bind.contains(name),
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
) -> Result<(), Error> {
    match (&value.from, &value.value) {
        (Some(from), None) => check_reference(
            node,
            from,
            &source.parse,
            &source.bind.keys().map(String::as_str).collect(),
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
