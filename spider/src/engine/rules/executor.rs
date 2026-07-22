use std::sync::Arc;

use crate::{config, net, spider};

mod bind;
mod build;
mod condition;
mod field;
mod value;

/// 解释执行当前 Rules node，并把产物送回统一 Spider/Tx 链路。
pub(crate) async fn execute<P>(
    spider: &P,
    schemas: Arc<crate::item::schema::Store>,
    config: &config::Config,
    request: &net::Request,
    response: &net::Response,
) -> Result<(), crate::Error>
where
    P: spider::Spider,
{
    let node_key = request.node_key();
    let node =
        config.graph.nodes.get(node_key).ok_or_else(|| {
            crate::Error::message(format!("rules node does not exist: {node_key}"))
        })?;

    let fields = field::parse(&node.parse, response).await?;
    let bind = bind::evaluate(node, request, response, &fields)?;
    let context = value::Context {
        request,
        response,
        fields: &fields,
        bind: &bind,
    };

    let mut requests = Vec::new();
    let mut output = None;
    for edge in config
        .graph
        .edges
        .iter()
        .filter(|edge| edge.from == node_key)
    {
        if !condition::matches(edge.when.as_deref(), &context)? {
            continue;
        }
        match edge.kind {
            crate::graph::edge::Kind::Request => {
                requests.extend(build::requests(config, edge, &context)?)
            }
            crate::graph::edge::Kind::Item => {
                let item = config.item.as_ref().ok_or_else(|| {
                    crate::Error::message("item edge requires top-level item config")
                })?;
                let schema = schemas.register(&item.schema).map_err(crate::Error::Item)?;
                if output.is_some() {
                    return Err(crate::Error::message(format!(
                        "rules node {node_key} has more than one item edge"
                    )));
                }
                output = Some((
                    build::item(item, edge, &context)?,
                    schema,
                    edge.function.as_deref().unwrap_or("item"),
                ));
            }
        }
    }

    if !requests.is_empty() {
        spider.tx().request(requests).await?;
    }
    if let Some((values, schema, function)) = output {
        let mut item =
            <P::Item as crate::item::Item>::from_values(values).map_err(crate::Error::Item)?;
        crate::item::Item::state_mut(&mut item).set_schema(Some(schema));
        let function = spider.item_fn(function).unwrap_or_else(|| {
            panic!("Rules item function is not registered by current Spider: {function}")
        });
        function.call(spider, item).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use indexmap::IndexMap;
    use serde_json::Value;

    use super::value::Context;
    use super::*;
    use crate::graph::rules::ValueRef;

    fn response(body: &str) -> net::Response {
        let request = net::Request::follow("https://example.com/books").unwrap();
        net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url: "https://example.com/books".to_string(),
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::from(body.to_string()),
        }
    }

    #[test]
    fn url_array_rejects_invalid_members_instead_of_dropping_them() {
        let error = build::extract_urls(serde_json::json!(["/one", 2])).unwrap_err();

        assert!(error.to_string().contains("non-empty strings"));
    }

    #[test]
    fn url_array_accepts_empty() {
        assert!(
            build::extract_urls(Value::Array(Vec::new()))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn request_array_aligns_vals_and_broadcasts_scalars() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index:
      parse:
        fields:
          links: {}
          titles: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: {from: $fields.links}
        vals:
          title: {from: $fields.titles}
          source: {from: $response.url}
"#,
            )
            .unwrap(),
        );
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::from([
            (
                "links".to_string(),
                serde_json::json!(["https://example.com/one", "https://example.com/two"]),
            ),
            ("titles".to_string(), serde_json::json!(["One", "Two"])),
        ]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let requests = build::requests(&config, &config.graph.edges[0], &context).unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].vals["idx"], Value::from(1));
        assert_eq!(requests[0].vals["title"], Value::from("One"));
        assert_eq!(
            requests[0].vals["source"],
            Value::from(response.url.clone())
        );
        assert_eq!(requests[1].vals["idx"], Value::from(2));
        assert_eq!(requests[1].vals["title"], Value::from("Two"));
        assert_eq!(requests[1].vals["source"], Value::from(response.url));
    }

    #[test]
    fn rules_descendants_receive_independent_cookie_snapshots() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index:
      parse:
        fields:
          links: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: {from: $fields.links}
"#,
            )
            .unwrap(),
        );
        let request = net::Request::follow("https://example.com/books").unwrap();
        let mut response = response("");
        let response_url = url::Url::parse(&response.url).unwrap();
        response
            .cookies
            .insert(&response_url, "sid", "shared")
            .unwrap();
        let fields = IndexMap::from([(
            "links".to_string(),
            serde_json::json!(["https://example.com/one", "https://example.com/two"]),
        )]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let mut requests = build::requests(&config, &config.graph.edges[0], &context).unwrap();
        let first_url = url::Url::parse(&requests[0].url).unwrap();
        let second_url = url::Url::parse(&requests[1].url).unwrap();
        assert_eq!(requests[0].cookies.get(&first_url, "sid"), Some("shared"));
        assert_eq!(requests[1].cookies.get(&second_url, "sid"), Some("shared"));

        requests[0]
            .cookies
            .insert(&first_url, "sid", "changed")
            .unwrap();
        assert_eq!(requests[0].cookies.get(&first_url, "sid"), Some("changed"));
        assert_eq!(requests[1].cookies.get(&second_url, "sid"), Some("shared"));
    }

    #[test]
    fn request_array_renders_target_templates_after_alignment() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index:
      parse:
        fields:
          links: {}
          titles: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: {from: $fields.links}
        headers:
          X-Row: "{idx}:{title}"
        body:
          kind: json
          data:
            position: "{idx}"
            title: "{title}"
        vals:
          title: {from: $fields.titles}
"#,
            )
            .unwrap(),
        );
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::from([
            (
                "links".to_string(),
                serde_json::json!(["https://example.com/one", "https://example.com/two"]),
            ),
            ("titles".to_string(), serde_json::json!(["One", "Two"])),
        ]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let requests = build::requests(&config, &config.graph.edges[0], &context).unwrap();

        assert_eq!(requests[0].headers.get("X-Row").unwrap(), "1:One");
        assert_eq!(requests[1].headers.get("X-Row").unwrap(), "2:Two");
        let net::Body::Json(first_body) = &requests[0].body else {
            panic!("expected a JSON body");
        };
        assert_eq!(first_body["position"], Value::from(1));
        assert_eq!(first_body["title"], Value::from("One"));
    }

    #[test]
    fn request_array_rejects_misaligned_vals() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index:
      parse:
        fields:
          links: {}
          titles: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: {from: $fields.links}
        vals:
          title: {from: $fields.titles}
"#,
            )
            .unwrap(),
        );
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::from([
            (
                "links".to_string(),
                serde_json::json!(["https://example.com/one", "https://example.com/two"]),
            ),
            ("titles".to_string(), serde_json::json!(["One"])),
        ]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let error = build::requests(&config, &config.graph.edges[0], &context).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("title has 1 values but url has 2")
        );
    }

    #[test]
    fn scalar_request_does_not_add_idx() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index:
      parse:
        fields:
          link: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: {from: $fields.link}
"#,
            )
            .unwrap(),
        );
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::from([("link".to_string(), Value::from("https://example.com/one"))]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let requests = build::requests(&config, &config.graph.edges[0], &context).unwrap();

        assert_eq!(requests.len(), 1);
        assert!(!requests[0].vals.contains_key("idx"));
    }

    #[test]
    fn empty_item_vals_preserve_parsed_and_bound_values() {
        let config = serde_json::from_value::<crate::item::Config>(serde_json::json!({
            "schema": {
                "fields": {
                    "null_value": {},
                    "empty_string": {},
                    "empty_array": {},
                    "empty_object": {},
                    "from_field": {}
                }
            }
        }))
        .unwrap();
        let edge = serde_json::from_value::<crate::graph::edge::Spec>(serde_json::json!({
            "from": "detail",
            "kind": "item",
            "vals": {
                "null_value": null,
                "empty_string": "",
                "empty_array": [],
                "empty_object": {}
            }
        }))
        .unwrap();
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::from([
            ("null_value".to_string(), Value::from("parsed-null")),
            ("empty_string".to_string(), Value::from("parsed-string")),
            ("empty_array".to_string(), Value::from("parsed-array")),
            ("empty_object".to_string(), Value::from("parsed-object")),
            ("from_field".to_string(), Value::from("parsed-field")),
        ]);
        let bind = IndexMap::from([
            ("null_value".to_string(), Value::from("bound-null")),
            ("empty_string".to_string(), Value::from("bound-string")),
            ("empty_array".to_string(), Value::from("bound-array")),
            ("empty_object".to_string(), Value::from("bound-object")),
        ]);
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let values = build::item(&config, &edge, &context).unwrap();

        assert_eq!(values["null_value"], Value::from("bound-null"));
        assert_eq!(values["empty_string"], Value::from("bound-string"));
        assert_eq!(values["empty_array"], Value::from("bound-array"));
        assert_eq!(values["empty_object"], Value::from("bound-object"));
        assert_eq!(values["from_field"], Value::from("parsed-field"));
    }

    #[test]
    fn zero_and_false_item_vals_override_base_values() {
        let config = serde_json::from_value::<crate::item::Config>(serde_json::json!({
            "schema": {
                "fields": {
                    "count": {},
                    "enabled": {}
                }
            }
        }))
        .unwrap();
        let edge = serde_json::from_value::<crate::graph::edge::Spec>(serde_json::json!({
            "from": "detail",
            "kind": "item",
            "vals": {
                "count": 0,
                "enabled": false
            }
        }))
        .unwrap();
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::new();
        let bind = IndexMap::from([
            ("count".to_string(), Value::from(7)),
            ("enabled".to_string(), Value::from(true)),
        ]);
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };

        let values = build::item(&config, &edge, &context).unwrap();

        assert_eq!(values["count"], Value::from(0));
        assert_eq!(values["enabled"], Value::from(false));
    }

    #[tokio::test]
    async fn extractor_fallback_uses_the_first_non_empty_result() {
        let parse = serde_json::from_value::<crate::graph::rules::Parse>(serde_json::json!({
            "fields": {
                "title": {
                    "required": true,
                    "extractors": [
                        {"kind": "css", "expr": "h2::text"},
                        {"kind": "css", "expr": "h1::text"}
                    ]
                }
            }
        }))
        .unwrap();

        let fields = field::parse(&parse, &response("<h1>Rust Book</h1>"))
            .await
            .unwrap();

        assert_eq!(fields["title"], Value::String("Rust Book".to_string()));
    }

    #[tokio::test]
    async fn extractor_errors_are_not_treated_as_empty_results() {
        let parse = serde_json::from_value::<crate::graph::rules::Parse>(serde_json::json!({
            "fields": {
                "title": {
                    "extractors": [
                        {"kind": "css", "expr": "h1["},
                        {"kind": "css", "expr": "h1::text"}
                    ]
                }
            }
        }))
        .unwrap();

        let error = field::parse(&parse, &response("<h1>Rust Book</h1>"))
            .await
            .unwrap_err();

        assert!(matches!(error, crate::Error::Selector(_)));
    }

    #[tokio::test]
    async fn parse_preserves_element_nodes_and_uses_match_cardinality() {
        let parse = serde_json::from_value::<crate::graph::rules::Parse>(serde_json::json!({
            "fields": {
                "images": {
                    "extractors": [{"kind": "css", "expr": "img"}]
                },
                "title": {
                    "extractors": [{"kind": "css", "expr": "h1::text"}]
                }
            }
        }))
        .unwrap();
        let response =
            response(r#"<h1>Book</h1><img src="/a.jpg" alt="A"><img src="/b.jpg" alt="B">"#);

        let fields = field::parse(&parse, &response).await.unwrap();

        assert_eq!(fields["title"], Value::String("Book".to_string()));
        assert_eq!(fields["images"].as_array().unwrap().len(), 2);
        assert_eq!(fields["images"][0]["attrs"]["src"], "/a.jpg");
        assert!(fields["images"][0]["html"].as_str().is_some());
        assert!(fields["images"][0]["text"].as_str().is_some());
    }

    #[test]
    fn template_renders_declared_field_and_request_values() {
        let mut request = net::Request::follow("https://example.com/books").unwrap();
        request.vals.insert("edition".to_string(), Value::from(2));
        let response = response("");
        let fields = IndexMap::from([("slug".to_string(), Value::String("rust-book".to_string()))]);
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };
        let vars = serde_json::from_value::<IndexMap<String, ValueRef>>(serde_json::json!({
            "slug": {"from": "$fields.slug"},
            "edition": {"from": "$vals.edition"}
        }))
        .unwrap();

        let rendered = bind::render("{slug}-v{edition}", &vars, &context).unwrap();

        assert_eq!(rendered, Value::String("rust-book-v2".to_string()));
    }

    #[test]
    fn template_does_not_reinterpret_resolved_braces() {
        let request = net::Request::follow("https://example.com/books").unwrap();
        let response = response("");
        let fields = IndexMap::new();
        let bind = IndexMap::new();
        let context = Context {
            request: &request,
            response: &response,
            fields: &fields,
            bind: &bind,
        };
        let vars = serde_json::from_value::<IndexMap<String, ValueRef>>(serde_json::json!({
            "first": {"value": "{second}"},
            "second": {"value": "value"}
        }))
        .unwrap();

        let rendered = bind::render("{first}-{second}", &vars, &context).unwrap();

        assert_eq!(rendered, Value::String("{second}-value".to_string()));
    }
}
