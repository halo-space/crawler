use std::collections::HashSet;

use crate::{config, graph};

pub(super) fn check(config: &graph::Config, item_configured: bool) -> Result<(), config::Error> {
    if config.nodes.keys().any(|name| name.trim().is_empty()) {
        return Err(config::Error::Message(
            "graph.nodes must not contain an empty node name".to_string(),
        ));
    }
    if config.nodes.contains_key("items") {
        return Err(config::Error::Message(
            "graph.nodes must not use the reserved name: items".to_string(),
        ));
    }
    for (name, node) in &config.nodes {
        if node
            .allowed_domains
            .as_ref()
            .is_some_and(|domains| domains.iter().any(|domain| domain.trim().is_empty()))
        {
            return Err(config::Error::Message(format!(
                "node {name} allowed_domains must not contain empty values"
            )));
        }
    }
    let mut item_sources = HashSet::new();
    for edge in &config.edges {
        if !config.nodes.contains_key(&edge.from) {
            return Err(config::Error::Message(format!(
                "edge.from does not exist in graph.nodes: {}",
                edge.from
            )));
        }
        match edge.kind {
            graph::edge::Kind::Request => {
                let request = edge.request.as_ref().ok_or_else(|| {
                    config::Error::Message("request edge requires request".to_string())
                })?;
                if request.node.trim().is_empty() {
                    return Err(config::Error::Message(format!(
                        "request edge from {} has an empty request.node",
                        edge.from
                    )));
                }
                if !config.nodes.contains_key(&request.node) {
                    return Err(config::Error::Message(format!(
                        "request node does not exist in graph.nodes: {}",
                        request.node
                    )));
                }
                if request.url.from.is_none() && request.url.value.is_none() {
                    return Err(config::Error::Message(format!(
                        "request edge from {} requires request.url",
                        edge.from
                    )));
                }
                if let Some(value) = request.url.value.as_ref() {
                    check_literal_url(&edge.from, value)?;
                }
                if request.vals.contains_key("idx") {
                    return Err(config::Error::Message(format!(
                        "request edge from {} must not define reserved val: idx",
                        edge.from
                    )));
                }
                if !edge.vals.is_empty() {
                    return Err(config::Error::Message(format!(
                        "request edge from {} must define vals inside request",
                        edge.from
                    )));
                }
                if edge.function.is_some() {
                    return Err(config::Error::Message(format!(
                        "request edge from {} must not define fn",
                        edge.from
                    )));
                }
                request.transport.validate_with(
                    &format!("request edge from {} to {}", edge.from, request.node),
                    |reference| valid_template(reference, &config.nodes[&edge.from]),
                )?;
            }
            graph::edge::Kind::Item => {
                if edge.request.is_some() {
                    return Err(config::Error::Message(format!(
                        "item edge from {} must not define request",
                        edge.from
                    )));
                }
                if !item_configured {
                    return Err(config::Error::Message(format!(
                        "item edge from {} requires top-level item config",
                        edge.from
                    )));
                }
                if !item_sources.insert(edge.from.as_str()) {
                    return Err(config::Error::Message(format!(
                        "node {} must not define more than one item edge",
                        edge.from
                    )));
                }
                if edge
                    .function
                    .as_ref()
                    .is_some_and(|function| function.trim().is_empty())
                {
                    return Err(config::Error::Message(format!(
                        "item edge from {} has an empty fn",
                        edge.from
                    )));
                }
            }
        }
    }
    Ok(())
}

fn check_literal_url(source: &str, value: &serde_json::Value) -> Result<(), config::Error> {
    let valid = match value {
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(values) => values
            .iter()
            .all(|value| value.as_str().is_some_and(|value| !value.is_empty())),
        serde_json::Value::Null => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(config::Error::Message(format!(
            "request edge from {source} url literal must be a string, string array, or null"
        )))
    }
}

fn valid_template(reference: &str, source: &graph::node::Config) -> bool {
    match reference.split_once('.') {
        None => !reference.is_empty(),
        Some(("fields", name)) => source.parse.fields.contains_key(name),
        Some(("bind", name)) => source.bind.contains_key(name),
        Some(("vals", name)) => !name.is_empty(),
        Some(("request", name)) => matches!(name, "url" | "node"),
        Some(("response", name)) => matches!(name, "url" | "status"),
        Some(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;

    #[test]
    fn rejects_reserved_items_node() {
        let error = Config::from_yaml(
            r#"
spider:
  name: reserved
  start: [{node: items, url: https://example.com}]
graph:
  nodes:
    items: {}
  edges: []
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("reserved name: items"));
    }

    #[test]
    fn rejects_undefined_request_transport_reference() {
        let error = Config::from_yaml(
            r#"
spider:
  name: transport
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: https://example.com/detail
        headers:
          X-Source: "{fields.missing}"
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("fields.missing"));
    }

    #[test]
    fn rejects_reserved_expansion_index() {
        let error = Config::from_yaml(
            r#"
spider:
  name: idx
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: https://example.com/detail
        vals:
          idx: 99
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("reserved val: idx"));
    }

    #[test]
    fn rejects_more_than_one_item_edge_per_source() {
        let error = Config::from_yaml(
            r#"
spider:
  name: items
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges:
    - {from: index, kind: item}
    - {from: index, kind: item, fn: save}
item:
  schema:
    fields: {}
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("more than one item edge"));
    }

    #[test]
    fn node_allows_explicit_unrestricted_domains_but_rejects_empty_names() {
        let unrestricted = r#"
spider:
  name: domains
  allowed_domains: [example.com]
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      allowed_domains: []
  edges: []
"#;
        assert!(Config::from_yaml(unrestricted).is_ok());

        let error = Config::from_yaml(
            &unrestricted.replace("allowed_domains: []", "allowed_domains: ['']"),
        )
        .unwrap_err();
        assert!(error.to_string().contains("allowed_domains"));
    }
}
