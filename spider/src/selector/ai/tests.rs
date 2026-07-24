use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::ai::{OpenAI, test_support};

fn response(body: impl Into<bytes::Bytes>) -> net::Response {
    net::Response::new(
        net::Request::follow("https://example.com").unwrap(),
        net::StatusCode(200),
        body,
    )
}

#[tokio::test]
async fn response_clones_concurrently_reuse_openai_and_request_json_objects() {
    let server = test_support::server_after_all_requests(vec![
        test_support::Reply::completion(Some(r#"{"title":"Rust"}"#)),
        test_support::Reply::completion(Some(r#"{"title":"Crawler"}"#)),
    ]);
    let openai = Arc::new(OpenAI::new(server.base_url(), "secret", "test-model").unwrap());
    let mut response = response("<h1>Rust</h1>");
    response.attach_ai(Some(Arc::clone(&openai)));
    let cloned = response.clone();
    assert!(Arc::ptr_eq(response.ai.as_ref().unwrap(), &openai));
    assert!(Arc::ptr_eq(cloned.ai.as_ref().unwrap(), &openai));

    let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            response.ai(r#"按 {"title":"xx"} 提取标题"#),
            cloned.ai(r#"按 {"title":"xx"} 再次提取标题"#),
        )
    })
    .await
    .expect("OpenAI provider serialized concurrent requests");
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

#[tokio::test]
async fn rejects_empty_and_oversized_inputs_without_a_provider_request() {
    let server = test_support::server_with(vec![test_support::Reply::completion(Some("{}"))]);
    let openai = Arc::new(OpenAI::new(server.base_url(), "secret", "model").unwrap());
    let mut empty = response("body");
    empty.attach_ai(Some(Arc::clone(&openai)));
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
    oversized.attach_ai(Some(Arc::clone(&openai)));
    assert_eq!(
        oversized.ai("extract JSON object").await.unwrap_err(),
        response_body_too_large()
    );
    assert_eq!(server.request_count(), 0);

    let mut oversized_expr = response("");
    oversized_expr.attach_ai(Some(Arc::clone(&openai)));
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
    expanded.attach_ai(Some(openai));
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
    let openai = Arc::new(OpenAI::new(server.base_url(), "secret", "model").unwrap());
    let mut response = response("body");
    response.attach_ai(Some(openai));
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
    let openai = Arc::new(OpenAI::new(base_url, "secret", "test-model").unwrap());
    let mut missing = response("body");
    missing.attach_ai(Some(openai));
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
        let openai = Arc::new(OpenAI::new(base_url, "secret", "test-model").unwrap());
        let mut invalid = response("body");
        invalid.attach_ai(Some(openai));
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
