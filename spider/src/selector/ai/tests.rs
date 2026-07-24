use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::instrument::WithSubscriber;

use super::*;

#[derive(Clone)]
struct Events {
    values: Arc<Mutex<Vec<String>>>,
    next_span: Arc<AtomicU64>,
}

impl Events {
    fn new(values: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            values,
            next_span: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl tracing::Subscriber for Events {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);

        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(&mut self.0, " {}={value:?}", field.name());
            }
        }

        let mut visitor = Visitor(event.metadata().target().to_string());
        event.record(&mut visitor);
        self.values.lock().unwrap().push(visitor.0);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

fn response(body: impl Into<bytes::Bytes>) -> net::Response {
    net::Response::new(
        net::Request::follow("https://example.com").unwrap(),
        net::StatusCode(200),
        body,
    )
}

#[test]
fn validates_client_configuration_and_hides_the_key() {
    let client = Client::new("https://example.com/v1/", "secret", "model").unwrap();
    assert_eq!(client.base_url, "https://example.com/v1");
    assert!(!format!("{client:?}").contains("secret"));

    for result in [
        Client::new("not-a-url", "secret", "model"),
        Client::new("https://user:pass@example.com/v1", "secret", "model"),
        Client::new("https://example.com/v1?token=url-secret", "secret", "model"),
        Client::new("https://example.com/v1#chat", "secret", "model"),
        Client::new("https://example.com/v1", "", "model"),
        Client::new("https://example.com/v1", "secret", ""),
        Client::new("https://example.com/v1", "secret\r\nx-leak: yes", "model"),
    ] {
        assert!(result.is_err());
    }
    assert!(Client::from_env("https://example.com/v1", "invalid-name", "model").is_err());
}

#[tokio::test]
async fn response_clones_concurrently_reuse_the_client_and_request_json_objects() {
    let server = test_support::server_after_all_requests(vec![
        test_support::Reply::completion(Some(r#"{"title":"Rust"}"#)),
        test_support::Reply::completion(Some(r#"{"title":"Crawler"}"#)),
    ]);
    let client = Arc::new(Client::new(server.base_url(), "secret", "test-model").unwrap());
    let mut response = response("<h1>Rust</h1>");
    response.attach_ai(Some(Arc::clone(&client)));
    let cloned = response.clone();
    assert!(Arc::ptr_eq(response.ai.as_ref().unwrap(), &client));
    assert!(Arc::ptr_eq(cloned.ai.as_ref().unwrap(), &client));

    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            response.ai(r#"按 {"title":"xx"} 提取标题"#),
            cloned.ai(r#"按 {"title":"xx"} 再次提取标题"#),
        )
    })
    .await
    .expect("AI client serialized concurrent provider requests");
    let mut titles = [
        first.unwrap()["title"].as_str().unwrap().to_string(),
        second.unwrap()["title"].as_str().unwrap().to_string(),
    ];
    titles.sort();
    assert_eq!(titles, ["Crawler", "Rust"]);

    for request in [
        server.request(Duration::from_secs(1)).unwrap(),
        server.request(Duration::from_secs(1)).unwrap(),
    ] {
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer secret")
        );
        assert!(!request.to_ascii_lowercase().contains("openai-organization"));
        assert!(!request.to_ascii_lowercase().contains("openai-project"));
        let body: Value = serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body["model"], Value::from("test-model"));
        assert_eq!(body["response_format"]["type"], Value::from("json_object"));
        let prompt = body["messages"][0]["content"].as_str().unwrap();
        assert!(prompt.contains("只能返回一个合法 JSON 对象"));
        assert!(prompt.contains("禁止返回数组"));
        assert!(prompt.contains("<h1>Rust</h1>"));
    }
    assert_eq!(server.request_count(), 2);
}

#[test]
fn from_env_is_resolved_when_the_client_is_built() {
    for (case, api_key) in [
        ("success", Some("construction-secret")),
        ("empty", Some("   ")),
        ("missing", None),
    ] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "selector::ai::tests::from_env_child",
                "--nocapture",
            ])
            .env("CRAWLER_AI_FROM_ENV_CHILD", case)
            .env("OPENAI_ORG_ID", "must-not-be-sent")
            .env("OPENAI_PROJECT_ID", "must-not-be-sent");
        if let Some(api_key) = api_key {
            command.env("CRAWLER_AI_FROM_ENV_KEY", api_key);
        } else {
            command.env_remove("CRAWLER_AI_FROM_ENV_KEY");
        }
        let status = command.status().unwrap();
        assert!(status.success(), "from_env child case failed: {case}");
    }
}

#[tokio::test]
async fn from_env_child() {
    let Ok(case) = std::env::var("CRAWLER_AI_FROM_ENV_CHILD") else {
        return;
    };
    match case.as_str() {
        "success" => {
            let server = test_support::server_with(vec![test_support::Reply::completion(Some(
                r#"{"ok":true}"#,
            ))]);
            let client = Arc::new(
                Client::from_env(server.base_url(), "CRAWLER_AI_FROM_ENV_KEY", "model").unwrap(),
            );
            let mut response = response("body");
            response.attach_ai(Some(client));
            response.ai(r#"按 {"ok":true} 输出"#).await.unwrap();
            let request = server.request(Duration::from_secs(1)).unwrap();
            let request = request.to_ascii_lowercase();
            assert!(request.contains("authorization: bearer construction-secret"));
            assert!(!request.contains("openai-organization"));
            assert!(!request.contains("openai-project"));
        }
        "empty" => {
            let error =
                Client::from_env("https://example.com/v1", "CRAWLER_AI_FROM_ENV_KEY", "model")
                    .unwrap_err();
            assert!(error.to_string().contains("is empty"));
        }
        "missing" => {
            let error =
                Client::from_env("https://example.com/v1", "CRAWLER_AI_FROM_ENV_KEY", "model")
                    .unwrap_err();
            assert!(error.to_string().contains("is not set"));
        }
        _ => panic!("unknown from_env child case"),
    }
}

#[tokio::test]
async fn provider_errors_are_bounded_and_do_not_enter_application_tracing() {
    let api_key = "provider-log-secret";
    let malformed = test_support::server_with(vec![test_support::Reply::error(
        400,
        format!("malformed {api_key}"),
    )]);
    let unavailable = test_support::server_with(vec![test_support::Reply::error(
        500,
        format!("server echoed {api_key}"),
    )]);
    let mut malformed_response = response("body");
    malformed_response.attach_ai(Some(Arc::new(
        Client::new(malformed.base_url(), api_key, "model").unwrap(),
    )));
    let mut unavailable_response = response("body");
    unavailable_response.attach_ai(Some(Arc::new(
        Client::new(unavailable.base_url(), api_key, "model").unwrap(),
    )));
    let events = Arc::new(Mutex::new(Vec::new()));

    async {
        tracing::info!("capture-probe");
        assert_eq!(
            malformed_response
                .ai("extract JSON object")
                .await
                .unwrap_err(),
            selector::Error::Ai("AI provider returned an invalid response".to_string())
        );
        assert_eq!(
            unavailable_response
                .ai("extract JSON object")
                .await
                .unwrap_err(),
            selector::Error::Ai("AI provider returned HTTP 500".to_string())
        );
    }
    .with_subscriber(Events::new(Arc::clone(&events)))
    .await;

    let events = events.lock().unwrap();
    assert!(events.iter().any(|event| event.contains("capture-probe")));
    assert!(
        events.iter().all(|event| !event.contains(api_key)),
        "provider response leaked into tracing: {events:?}"
    );
    assert_eq!(malformed.request_count(), 1);
    assert_eq!(unavailable.request_count(), 1);
}

#[tokio::test]
async fn rejects_an_oversized_provider_response_before_dependency_buffering() {
    let server = test_support::server_with(vec![test_support::Reply::declared_length(
        200,
        transport::MAX_BODY_BYTES + 1,
    )]);
    let client = Arc::new(Client::new(server.base_url(), "secret", "model").unwrap());
    let mut response = response("body");
    response.attach_ai(Some(client));

    assert_eq!(
        response.ai("extract JSON object").await.unwrap_err(),
        selector::Error::Ai(format!(
            "AI provider decoded response body exceeds the {}-byte limit",
            transport::MAX_BODY_BYTES
        ))
    );
    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn bounds_compressed_success_and_declared_error_provider_bodies() {
    let oversized = vec![b'x'; transport::MAX_BODY_BYTES + 1];
    let server = test_support::server_with(vec![
        test_support::Reply::gzip(200, &oversized),
        test_support::Reply::declared_length(500, transport::MAX_BODY_BYTES + 1),
    ]);
    let client = Arc::new(Client::new(server.base_url(), "secret", "model").unwrap());
    let mut response = response("body");
    response.attach_ai(Some(client));
    let expected = selector::Error::Ai(format!(
        "AI provider decoded response body exceeds the {}-byte limit",
        transport::MAX_BODY_BYTES
    ));

    assert_eq!(
        response.ai("extract JSON object").await.unwrap_err(),
        expected
    );
    assert_eq!(
        response.ai("extract JSON object").await.unwrap_err(),
        expected
    );
    assert_eq!(server.request_count(), 2);
}

#[tokio::test]
async fn rejects_empty_and_oversized_inputs_without_a_provider_request() {
    let server = test_support::server_with(vec![test_support::Reply::completion(Some("{}"))]);
    let client = Arc::new(Client::new(server.base_url(), "secret", "model").unwrap());
    let mut empty = response("body");
    empty.attach_ai(Some(Arc::clone(&client)));
    assert!(
        empty
            .ai(" \n\t ")
            .await
            .unwrap_err()
            .to_string()
            .contains("empty")
    );
    assert_eq!(server.request_count(), 0);

    let mut oversized = response(vec![b'x'; MAX_RESPONSE_BODY_BYTES + 1]);
    oversized.attach_ai(Some(Arc::clone(&client)));
    assert_eq!(
        oversized.ai("extract JSON object").await.unwrap_err(),
        response_body_too_large()
    );
    assert_eq!(server.request_count(), 0);

    let mut oversized_expr = response("");
    oversized_expr.attach_ai(Some(Arc::clone(&client)));
    assert_eq!(
        oversized_expr
            .ai(&"x".repeat(MAX_PROMPT_BYTES))
            .await
            .unwrap_err(),
        prompt_too_large()
    );
    assert_eq!(server.request_count(), 0);

    let mut expanded = response(vec![0x80; MAX_PROMPT_BYTES / 2]);
    expanded
        .headers
        .try_set("content-type", "text/html; charset=windows-1252")
        .unwrap();
    expanded.attach_ai(Some(client));
    assert_eq!(
        expanded.ai("extract JSON object").await.unwrap_err(),
        prompt_too_large()
    );
    assert_eq!(server.request_count(), 0);
}

#[tokio::test]
async fn rejects_non_object_results() {
    let server = test_support::server_with(
        ["[]", r#""text""#, "null", "1", "true"]
            .into_iter()
            .map(|content| test_support::Reply::completion(Some(content)))
            .collect(),
    );
    let client = Arc::new(Client::new(server.base_url(), "secret", "model").unwrap());
    let mut response = response("body");
    response.attach_ai(Some(client));
    for _ in 0..5 {
        assert_eq!(
            response.ai("extract JSON object").await.unwrap_err(),
            selector::Error::Ai("model content must be a JSON object".to_string())
        );
    }
    assert_eq!(server.request_count(), 5);
}

#[tokio::test]
async fn rejects_missing_and_invalid_json_content() {
    let (base_url, _) = test_support::server(None);
    let client = Arc::new(Client::new(base_url, "secret", "test-model").unwrap());
    let mut missing = response("body");
    missing.attach_ai(Some(client));
    assert!(
        missing
            .ai("prompt")
            .await
            .unwrap_err()
            .to_string()
            .contains("no content")
    );

    for content in ["not-json", "```json\n{}\n```", "result: {}", "{} {}"] {
        let (base_url, _) = test_support::server(Some(content));
        let client = Arc::new(Client::new(base_url, "secret", "test-model").unwrap());
        let mut invalid = response("body");
        invalid.attach_ai(Some(client));
        assert!(
            invalid
                .ai("prompt")
                .await
                .unwrap_err()
                .to_string()
                .contains("not valid JSON")
        );
    }
}
