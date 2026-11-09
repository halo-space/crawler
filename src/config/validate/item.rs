use crate::{config, graph, item};

pub(super) fn check(
    config: Option<&item::Config>,
    graph: &graph::Config,
) -> Result<(), config::Error> {
    let Some(config) = config else {
        return Ok(());
    };
    item::schema::Store::new()
        .register(&config.schema)
        .map_err(|error| config::Error::Message(format!("invalid item schema: {error}")))?;
    let schema_fields = config
        .schema_fields()
        .map_err(|error| config::Error::Message(error.to_string()))?;
    for (node_name, node) in &graph.nodes {
        for (name, field) in &node.fields {
            if field.download && field.kind == item::config::Kind::Text {
                return Err(config::Error::Message(format!(
                    "node {node_name} field {name} download requires kind image, video, or audio"
                )));
            }
            if field.kind == item::config::Kind::Text {
                continue;
            }
            let schema = schema_fields.get(name).ok_or_else(|| {
                config::Error::Message(format!(
                    "node {node_name} field {name} is missing from item.schema.fields"
                ))
            })?;
            if schema.get("type").and_then(serde_json::Value::as_str) != Some("array") {
                return Err(config::Error::Message(format!(
                    "node {node_name} media field {name} must use halo-validator type array"
                )));
            }
        }
    }
    for edge in graph
        .edges
        .iter()
        .filter(|edge| edge.kind == graph::edge::Kind::Item)
    {
        if let Some(name) = edge
            .vals
            .keys()
            .find(|name| !schema_fields.contains_key(*name))
        {
            return Err(config::Error::Message(format!(
                "item edge from {} contains unknown field: {name}",
                edge.from
            )));
        }
    }
    Ok(())
}
