pub use crate::error::config::Error;

mod validate;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::graph;
use crate::net::Request;
use crate::spider;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub spider: spider::Config,
    pub graph: graph::Config,
    #[serde(default)]
    pub item: Option<crate::item::Config>,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("spider", &self.spider.name)
            .field("nodes", &self.graph.nodes.len())
            .field("edges", &self.graph.edges.len())
            .field("has_item", &self.item.is_some())
            .finish()
    }
}

impl Config {
    pub async fn load(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        let content = tokio::fs::read_to_string(path).await?;
        Self::from_yaml(&content)
    }

    pub fn from_yaml(content: &str) -> Result<Self, Error> {
        let config = serde_yaml::from_str::<Self>(content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), Error> {
        validate::check(self)
    }

    pub fn next_trace_id(&self) -> String {
        crate::trace::next_id(&self.spider.name)
    }

    pub fn initial_requests(
        &self,
        task_id: impl Into<String>,
        trace_id: impl Into<String>,
        vals: HashMap<String, Value>,
    ) -> Result<Vec<Request>, Error> {
        self.validate()?;

        let task_id = task_id.into();
        let trace_id = trace_id.into();
        self.spider
            .start
            .iter()
            .map(|spec| {
                let mut request = Request::follow(start_url(spec, &vals)?)
                    .map_err(|error| Error::Message(error.to_string()))?;
                request.set_node(spec.node.clone());
                request.task_id.clone_from(&task_id);
                request.trace_id.clone_from(&trace_id);
                request.vals.clone_from(&vals);
                for (name, value) in &spec.vals {
                    request
                        .vals
                        .insert(name.clone(), start_value(value, &vals)?);
                }
                request.priority = spec.priority.or(self.spider.priority).unwrap_or_default();
                let request_url = request.url.clone();
                let request_node = request.node_key().to_string();
                let request_vals = request.vals.clone();
                spec.transport.apply_with(&mut request, |name| match name {
                    "request.url" => Some(Value::String(request_url.clone())),
                    "request.node" => Some(Value::String(request_node.clone())),
                    name if !name.contains('.') => request_vals.get(name).cloned(),
                    name => name
                        .strip_prefix("vals.")
                        .and_then(|name| request_vals.get(name).cloned()),
                })?;
                Ok(request)
            })
            .collect()
    }
}

fn start_url(
    spec: &crate::graph::request::Spec,
    vals: &HashMap<String, Value>,
) -> Result<String, Error> {
    let value = start_value(&spec.url, vals)?;
    match value {
        Value::String(url) if !url.is_empty() => Ok(url),
        _ => Err(Error::Message(
            "spider.start url must be a non-empty string".to_string(),
        )),
    }
}

fn start_value(
    reference: &crate::graph::rules::ValueRef,
    vals: &HashMap<String, Value>,
) -> Result<Value, Error> {
    if let Some(value) = &reference.value {
        return Ok(value.clone());
    }
    let Some(path) = reference.from.as_deref() else {
        return Err(Error::Message(
            "request value requires from or value".to_string(),
        ));
    };
    let name = path
        .strip_prefix("$vals.")
        .ok_or_else(|| Error::Message(format!("unsupported start reference: {path}")))?;
    vals.get(name)
        .cloned()
        .ok_or_else(|| Error::Message(format!("undefined start value: {path}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{Body, Method};

    const RULES: &str = r#"
spider:
  name: books
  priority: 7
  start:
    - node: index
      url: https://example.com/
      protocol: http
      download_mode: http
      method: POST
      timeout: 30000
      max_body_bytes: 1048576
      dont_filter: true
      headers:
        Accept: text/html
      cookies:
        region: cn
      body:
        kind: json
        data:
          page: 1

graph:
  nodes:
    index:
      parse: {}
      bind: {}
  edges: []

item:
  schema:
    fields: {}
"#;

    #[test]
    fn unknown_dsl_fields_are_rejected() {
        let rules = RULES.replace("name: books", "name: books\n  allow_domains: [example.com]");

        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("allow_domains"));
    }

    #[test]
    fn invalid_css_options_are_rejected_during_loading() {
        let rules = r#"
spider:
  name: healing
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      parse:
        fields:
          title:
            extractors:
              - kind: css
                expr: h1.title::text
                args:
                  healing:
                    min: 1.2
  edges: []
"#;
        let error = Config::from_yaml(rules).unwrap_err();
        assert!(error.to_string().contains("healing min"));

        let unknown = rules.replace("min: 1.2", "min: 0.8\n                    weight: 2");
        let error = Config::from_yaml(&unknown).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn ai_extractor_only_accepts_a_prompt() {
        let rules = r#"
spider:
  name: ai
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      parse:
        fields:
          article:
            extractors:
              - kind: ai
                expr: 提取文章并返回 JSON
  edges: []
"#;
        assert!(Config::from_yaml(rules).is_ok());

        let old_args = rules.replace(
            "                expr: 提取文章并返回 JSON",
            "                expr: 提取文章并返回 JSON\n                args:\n                  base_url: https://api.example.com/v1\n                  api_key: env:OPENAI_API_KEY\n                  model_name: model",
        );
        let error = Config::from_yaml(&old_args).unwrap_err();
        let error = error.to_string();
        assert!(error.contains("unknown field"));
        assert!(error.contains("args"));

        for provider_field in [
            "base_url: https://api.example.com/v1",
            "api_key: env:OPENAI_API_KEY",
            "model_name: model",
        ] {
            let rules = rules.replace(
                "                expr: 提取文章并返回 JSON",
                &format!(
                    "                expr: 提取文章并返回 JSON\n                {provider_field}"
                ),
            );
            let error = Config::from_yaml(&rules).unwrap_err();
            assert!(error.to_string().contains("unknown field"));
        }
    }

    #[test]
    fn ai_extractor_prompt_cannot_be_empty() {
        let rules = r#"
spider:
  name: ai
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      parse:
        fields:
          article:
            extractors:
              - kind: ai
                expr: "  "
  edges: []
"#;

        let error = Config::from_yaml(rules).unwrap_err();
        assert!(error.to_string().contains("extractor expr is empty"));
    }

    #[test]
    fn extractors_survive_snapshot_round_trip() {
        let rules = r#"
spider:
  name: selectors
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      parse:
        fields:
          title:
            extractors:
              - kind: css
                expr: h1.title::text
                args:
                  healing:
                    min: 0.7
          article:
            extractors:
              - kind: ai
                expr: 提取文章并返回 JSON
  edges: []
"#;
        let config = Config::from_yaml(rules).unwrap();
        let encoded = serde_json::to_value(crate::trace::Snapshot::rules("task", config)).unwrap();
        assert_eq!(
            encoded["dsl"]["graph"]["nodes"]["index"]["parse"]["fields"]["article"]["extractors"]
                [0],
            serde_json::json!({
                "kind": "ai",
                "expr": "提取文章并返回 JSON",
            })
        );
        let restored = serde_json::from_value::<crate::trace::Snapshot>(encoded)
            .unwrap()
            .dsl
            .unwrap();
        let fields = &restored.graph.nodes["index"].parse.fields;

        let graph::rules::Extractor::Css { args, .. } = &fields["title"].extractors[0] else {
            panic!("title extractor must remain CSS");
        };
        assert_eq!(args.healing().unwrap().min(), 0.7);

        let graph::rules::Extractor::Ai { expr } = &fields["article"].extractors[0] else {
            panic!("article extractor must remain AI");
        };
        assert_eq!(expr, "提取文章并返回 JSON");
    }

    #[test]
    fn one_start_url_generates_one_initial_request() {
        let config = Config::from_yaml(RULES).unwrap();
        let requests = config
            .initial_requests("task-1", "trace-1", HashMap::new())
            .unwrap();

        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.task_id, "task-1");
        assert_eq!(request.trace_id, "trace-1");
        assert_eq!(request.node_key(), "index");
        assert_eq!(request.priority, 7);
        assert_eq!(request.method, Method::Post);
        assert_eq!(request.timeout, Some(30_000));
        assert_eq!(request.max_body_bytes, Some(1_048_576));
        assert!(request.dont_filter);
        assert_eq!(request.headers.get("Accept").unwrap(), "text/html");
        assert_eq!(
            request
                .cookies
                .get(&url::Url::parse(&request.url).unwrap(), "region"),
            Some("cn")
        );
        assert!(matches!(request.body, Body::Json(_)));
    }

    #[test]
    fn trace_ids_use_uuid_v7() {
        let config = Config::from_yaml(RULES).unwrap();
        let first = config.next_trace_id();
        let second = config.next_trace_id();

        assert_ne!(first, second);
        let uuid = first.strip_prefix("trace_books_").unwrap();
        let uuid = uuid::Uuid::parse_str(uuid).unwrap();
        assert_eq!(uuid.get_version(), Some(uuid::Version::SortRand));
    }

    #[test]
    fn initial_request_count_matches_start_specs() {
        let rules = RULES.replace(
            "    - node: index\n      url: https://example.com/",
            "    - node: index\n      url: https://example.com/one\n    - node: index\n      url: https://example.com/two",
        );
        let config = Config::from_yaml(&rules).unwrap();
        let requests = config
            .initial_requests("task-1", "trace-1", HashMap::new())
            .unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].trace_id, requests[1].trace_id);
        assert_ne!(requests[0].id, requests[1].id);
        assert_ne!(requests[0].url, requests[1].url);
    }

    #[test]
    fn missing_start_spec_node_is_rejected() {
        let rules = RULES.replace("    - node: index", "    - node: missing");
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("spider.start node"));
    }

    #[test]
    fn item_length_validator_requires_a_non_negative_integer() {
        let rules = RULES.replace(
            "    fields: {}",
            "    fields:\n      title:\n        type: string\n        rules:\n          - {len: 1.5}",
        );
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("invalid item schema"));
    }

    #[test]
    fn media_processing_field_requires_array_schema() {
        let rules = RULES.replace(
            "item:\n  schema:\n    fields: {}",
            "item:\n  fields:\n    cover:\n      kind: image\n  schema:\n    fields:\n      cover:\n        type: string",
        );

        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("must use validator type array"));
    }

    #[test]
    fn bind_forward_reference_is_rejected() {
        let rules = RULES.replace(
            "      bind: {}",
            "      bind:\n        first:\n          kind: pipeline\n          from: $bind.second\n        second:\n          kind: pipeline\n          from: $response.url",
        );
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("$bind.second"));
    }

    #[test]
    fn bind_template_is_validated_before_execution() {
        let rules = RULES.replace(
            "      bind: {}",
            "      bind:\n        title:\n          kind: template\n          template: '{missing}'\n          vars: {}",
        );
        let error = Config::from_yaml(&rules).unwrap_err();
        assert!(error.to_string().contains("template variable is undefined"));

        let malformed = rules.replace("'{missing}'", "'{missing'");
        let error = Config::from_yaml(&malformed).unwrap_err();
        assert!(error.to_string().contains("invalid template"));
    }

    #[test]
    fn initial_request_templates_render_from_start_vals() {
        let rules = RULES
            .replace("Accept: text/html", "Accept: \"{content_type}\"")
            .replace("region: cn", "region: \"{region}\"")
            .replace("page: 1", "page: \"{page}\"");
        let config = Config::from_yaml(&rules).unwrap();
        let vals = HashMap::from([
            ("content_type".to_string(), Value::from("application/json")),
            ("region".to_string(), Value::from("cn")),
            ("page".to_string(), Value::from(2)),
        ]);

        let requests = config.initial_requests("task-1", "trace-1", vals).unwrap();

        assert_eq!(
            requests[0].headers.get("Accept").unwrap(),
            "application/json"
        );
        assert_eq!(
            requests[0]
                .cookies
                .get(&url::Url::parse(&requests[0].url).unwrap(), "region"),
            Some("cn")
        );
        let Body::Json(body) = &requests[0].body else {
            panic!("expected a JSON body");
        };
        assert_eq!(body["page"], Value::from(2));
    }

    #[test]
    fn invalid_request_template_is_rejected_during_validation() {
        let rules = RULES.replace("Accept: text/html", "Accept: \"{content_type\"");
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("invalid template"));
    }

    #[test]
    fn transform_arguments_are_validated_before_execution() {
        let rules = RULES.replace(
            "      bind: {}",
            "      bind:\n        url:\n          kind: pipeline\n          from: $response.url\n          transforms:\n            - {kind: trim, unexpected: true}",
        );
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("does not accept arguments"));
    }

    #[test]
    fn url_join_reference_is_validated_before_execution() {
        let rules = RULES.replace(
            "      bind: {}",
            "      bind:\n        url:\n          kind: pipeline\n          from: $response.url\n          transforms:\n            - {kind: url_join, base_url: $bind.missing}",
        );
        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("$bind.missing"));
    }

    #[test]
    fn url_join_requires_a_usable_literal_base_url() {
        let rules = RULES.replace(
            "      bind: {}",
            "      bind:\n        url:\n          kind: pipeline\n          from: $response.url\n          transforms:\n            - {kind: url_join, base_url: 'mailto:test@example.com'}",
        );

        let error = Config::from_yaml(&rules).unwrap_err();

        assert!(error.to_string().contains("cannot be used as a base URL"));
    }
}
