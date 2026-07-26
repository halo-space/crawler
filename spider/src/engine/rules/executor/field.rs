use indexmap::IndexMap;
use serde_json::Value;

use super::value;
use crate::{graph, net, selector};

/// 按字段声明依次尝试 extractor，并生成当前 node 的字段值。
pub(super) async fn parse(
    parse: &graph::rules::Parse,
    response: &net::Response,
) -> Result<IndexMap<String, Value>, crate::Error> {
    let mut soup = None;
    let mut document = None;
    let mut fields = IndexMap::new();
    for (name, field) in &parse.fields {
        let mut field_value = None;
        for extractor in &field.extractors {
            let selection = match extractor {
                graph::rules::Extractor::Css { expr, args } => {
                    if soup.is_none() {
                        soup = Some(response.css()?);
                    }
                    let (expr, output) =
                        graph::rules::parse_css_output(expr).map_err(selector::Error::Css)?;
                    selector::css::select(
                        soup.as_ref().expect("CSS document initialized above"),
                        expr,
                        args,
                    )
                    .map(|nodes| {
                        nodes
                            .into_iter()
                            .filter_map(|node| css_value(node, output))
                            .filter(|value| !value::is_empty(value))
                            .collect()
                    })
                }
                graph::rules::Extractor::Regex { expr, .. } => {
                    selector::regex::select(response, expr)
                        .map(|values| values.into_iter().map(Value::String).collect::<Vec<_>>())
                }
                graph::rules::Extractor::Json { expr } => {
                    if document.is_none() {
                        document = Some(response.json::<Value>()?);
                    }
                    selector::json::select(
                        document.as_ref().expect("JSON document initialized above"),
                        expr,
                    )
                    .map(|values| values.into_iter().cloned().collect())
                }
                graph::rules::Extractor::Ai { expr } => response.ai(expr).await.map(|value| {
                    if value::is_empty(&value) {
                        Vec::new()
                    } else {
                        vec![value]
                    }
                }),
            };
            match selection {
                Ok(values) if !values.is_empty() => {
                    let selected = collapse(values);
                    if value::is_empty(&selected) {
                        continue;
                    }
                    field_value = Some(selected);
                    break;
                }
                Ok(_) => {}
                Err(error) => return Err(crate::Error::Selector(error)),
            }
        }

        let value = match field_value {
            Some(value) if !value::is_empty(&value) => value,
            _ if field.required => {
                return Err(crate::Error::message(format!(
                    "required field is empty: {name}"
                )));
            }
            _ => field.default.clone().unwrap_or(Value::Null),
        };
        fields.insert(name.clone(), value);
    }
    Ok(fields)
}

fn css_value(node: scrape_core::Tag<'_>, output: graph::rules::CssOutput<'_>) -> Option<Value> {
    match output {
        graph::rules::CssOutput::Element => Some(serde_json::json!({
            "html": node.outer_html(),
            "text": node.text(),
            "attrs": node.attrs().cloned().unwrap_or_default(),
        })),
        graph::rules::CssOutput::Text => Some(Value::String(node.text())),
        graph::rules::CssOutput::Attribute(name) => {
            node.get(name).map(|value| Value::String(value.to_string()))
        }
    }
}

fn collapse(mut values: Vec<Value>) -> Value {
    match values.len() {
        0 => Value::Null,
        1 => values.pop().unwrap_or(Value::Null),
        _ => Value::Array(values),
    }
}
#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn response(body: &str) -> net::Response {
        net::Response {
            request: net::Request::follow("https://example.com").unwrap(),
            url: "https://example.com".to_string(),
            status: net::StatusCode(200),
            reason: None,
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::from(body.to_string()),
            vals: Default::default(),
            kwargs: Default::default(),
            middlewares: Vec::new(),
            ai: None,
        }
    }

    fn response_with_ai(body: &str, output: &str) -> net::Response {
        let (base_url, _) = crate::ai::server::start(Some(output));
        let openai = crate::ai::OpenAI::new(base_url, "secret", "test-model").unwrap();
        let mut response = response(body);
        response.attach_ai(Some(std::sync::Arc::new(openai)));
        response
    }

    fn healing(min: f64) -> selector::css::Config {
        selector::css::Config::default()
            .with_healing(selector::css::healing::Config::new(min).unwrap())
    }

    fn eastmoney() -> net::Response {
        response(
            r#"{
                "rc": 0,
                "empty": null,
                "mixed": [null, "", "601398", []],
                "data": {
                    "total": 3,
                    "diff": [
                        {"f2": 6.61, "f12": "601398", "f14": "工商银行"},
                        {"f2": 46.15, "f12": "600036", "f14": "招商银行"},
                        {"f2": 6.36, "f12": "600030", "f14": "中信证券"}
                    ]
                }
            }"#,
        )
    }

    #[test]
    fn parses_compatible_css_outputs() {
        assert!(matches!(
            graph::rules::parse_css_output("article").unwrap(),
            ("article", graph::rules::CssOutput::Element)
        ));
        assert!(matches!(
            graph::rules::parse_css_output("article h2::text").unwrap(),
            ("article h2", graph::rules::CssOutput::Text)
        ));
        assert!(matches!(
            graph::rules::parse_css_output("a::attr(href)").unwrap(),
            ("a", graph::rules::CssOutput::Attribute("href"))
        ));
    }

    #[tokio::test]
    async fn rules_css_uses_public_healing_and_preserves_output_semantics() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "title".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![graph::rules::Extractor::css("h2.title::text", healing(0.7))],
                },
            )]),
        };
        let fields = parse(&spec, &response("<h2 class='titles'>Recovered</h2>"))
            .await
            .unwrap();
        assert_eq!(fields["title"], Value::from("Recovered"));
    }

    #[tokio::test]
    async fn rules_healing_preserves_attribute_and_element_outputs() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([
                (
                    "href".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::css(
                            "a.entry::attr(href)",
                            healing(0.7),
                        )],
                    },
                ),
                (
                    "node".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::css("a.entry", healing(0.7))],
                    },
                ),
            ]),
        };
        let fields = parse(&spec, &response("<a class='entries' href='/one'>One</a>"))
            .await
            .unwrap();
        assert_eq!(fields["href"], Value::from("/one"));
        assert_eq!(fields["node"]["text"], Value::from("One"));
        assert_eq!(fields["node"]["attrs"]["href"], Value::from("/one"));
    }

    #[tokio::test]
    async fn below_min_healing_continues_to_next_extractor() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "title".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![
                        graph::rules::Extractor::css("article#target.title::text", healing(1.0)),
                        graph::rules::Extractor::regex("<h2>(.*?)</h2>"),
                    ],
                },
            )]),
        };
        let fields = parse(&spec, &response("<h2>Fallback</h2>")).await.unwrap();
        assert_eq!(fields["title"], Value::from("Fallback"));
    }

    #[tokio::test]
    async fn rules_json_preserves_scalar_object_and_multiple_values() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([
                (
                    "total".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::json("$.data.total")],
                    },
                ),
                (
                    "first".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::json("$.data.diff[0]")],
                    },
                ),
                (
                    "codes".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::json("$.data.diff[*].f12")],
                    },
                ),
                (
                    "mixed".to_string(),
                    graph::rules::Field {
                        required: true,
                        default: None,
                        extractors: vec![graph::rules::Extractor::json("$.mixed[*]")],
                    },
                ),
            ]),
        };

        let fields = parse(&spec, &eastmoney()).await.unwrap();

        assert_eq!(fields["total"], Value::from(3));
        assert_eq!(fields["first"]["f14"], Value::from("工商银行"));
        assert_eq!(
            fields["codes"],
            serde_json::json!(["601398", "600036", "600030"])
        );
        assert_eq!(fields["mixed"], serde_json::json!([null, "", "601398", []]));
    }

    #[tokio::test]
    async fn empty_json_selection_continues_to_next_extractor() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "code".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![
                        graph::rules::Extractor::json("$.empty"),
                        graph::rules::Extractor::json("$.data.diff[0].f12"),
                    ],
                },
            )]),
        };

        let fields = parse(&spec, &eastmoney()).await.unwrap();

        assert_eq!(fields["code"], Value::from("601398"));
    }

    #[tokio::test]
    async fn invalid_json_body_and_path_return_typed_errors() {
        let valid_path = graph::rules::Parse {
            fields: IndexMap::from([(
                "code".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![graph::rules::Extractor::json("$.data.diff[0].f12")],
                },
            )]),
        };
        let invalid_path = graph::rules::Parse {
            fields: IndexMap::from([(
                "code".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![graph::rules::Extractor::json("$.data[")],
                },
            )]),
        };

        assert!(matches!(
            parse(&valid_path, &response("not-json")).await.unwrap_err(),
            crate::Error::Net(net::Error::Json(_))
        ));
        assert!(matches!(
            parse(&invalid_path, &eastmoney()).await.unwrap_err(),
            crate::Error::Selector(selector::Error::Json(_))
        ));
    }

    #[tokio::test]
    async fn rules_ai_uses_response_selector_and_preserves_json() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "article".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![graph::rules::Extractor::ai("提取文章并返回 JSON")],
                },
            )]),
        };
        let response = response_with_ai(
            "<article>Rust</article>",
            r#"{"title":"Rust","author":"Ferris"}"#,
        );
        let fields = parse(&spec, &response).await.unwrap();
        assert_eq!(fields["article"]["title"], Value::from("Rust"));
        assert_eq!(fields["article"]["author"], Value::from("Ferris"));
    }

    #[tokio::test]
    async fn empty_ai_json_continues_to_non_css_extractor() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "title".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![
                        graph::rules::Extractor::ai("提取标题并返回 JSON"),
                        graph::rules::Extractor::regex("<h1>(.*?)</h1>"),
                    ],
                },
            )]),
        };
        let response = response_with_ai("<h1>Fallback</h1>", "{}");
        let fields = parse(&spec, &response).await.unwrap();
        assert_eq!(fields["title"], Value::from("Fallback"));
    }

    #[tokio::test]
    async fn ai_extractor_without_provider_returns_an_explicit_error() {
        let spec = graph::rules::Parse {
            fields: IndexMap::from([(
                "title".to_string(),
                graph::rules::Field {
                    required: true,
                    default: None,
                    extractors: vec![graph::rules::Extractor::ai("提取标题并返回 JSON")],
                },
            )]),
        };

        let error = parse(&spec, &response("<h1>Rust</h1>")).await.unwrap_err();

        assert!(error.to_string().contains("AI provider is not configured"));
    }

    #[test]
    fn rejects_invalid_compatible_css_outputs() {
        for expr in ["::text", "article::attr()", "article::attr(href"] {
            assert!(graph::rules::parse_css_output(expr).is_err());
        }
    }
}
