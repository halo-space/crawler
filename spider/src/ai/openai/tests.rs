use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::instrument::WithSubscriber;

use super::*;
use crate::ai::{self, test_support, transport};

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

#[test]
fn validates_configuration_and_hides_the_key() {
    let openai = OpenAI::new("https://example.com/v1/", "secret", "model").unwrap();
    assert_eq!(openai.base_url, "https://example.com/v1");
    assert!(!format!("{openai:?}").contains("secret"));
    let config = Config::new("https://example.com/v1".to_string(), "secret".to_string()).unwrap();
    assert!(config.headers()[AUTHORIZATION].is_sensitive());

    for result in [
        OpenAI::new("not-a-url", "secret", "model"),
        OpenAI::new("https://user:pass@example.com/v1", "secret", "model"),
        OpenAI::new("https://example.com/v1?token=url-secret", "secret", "model"),
        OpenAI::new("https://example.com/v1#chat", "secret", "model"),
        OpenAI::new("https://example.com/v1", "", "model"),
        OpenAI::new("https://example.com/v1", "secret", ""),
        OpenAI::new("https://example.com/v1", "secret\r\nx-leak: yes", "model"),
    ] {
        assert!(result.is_err());
    }
}

#[test]
fn ambient_provider_environment_is_ignored() {
    let status = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "ai::openai::tests::ambient_provider_environment_child",
            "--nocapture",
        ])
        .env("CRAWLER_AI_AMBIENT_ENVIRONMENT_CHILD", "1")
        .env("OPENAI_BASE_URL", "https://ambient.invalid/v1")
        .env_remove("OPENAI_API_KEY")
        .env("OPENAI_ADMIN_KEY", "must-not-be-read")
        .env("OPENAI_ORG_ID", "must-not-be-sent")
        .env("OPENAI_PROJECT_ID", "must-not-be-sent")
        .status()
        .unwrap();

    assert!(status.success());
}

#[tokio::test]
async fn ambient_provider_environment_child() {
    if std::env::var_os("CRAWLER_AI_AMBIENT_ENVIRONMENT_CHILD").is_none() {
        return;
    }
    let server = test_support::server_with(vec![test_support::Reply::completion(Some(
        r#"{"ok":true}"#,
    ))]);
    let events = Arc::new(Mutex::new(Vec::new()));
    async {
        let openai = OpenAI::new(server.base_url(), "construction-secret", "model").unwrap();
        assert_eq!(
            openai.complete("prompt".to_string()).await.unwrap(),
            r#"{"ok":true}"#
        );
    }
    .with_subscriber(Events::new(Arc::clone(&events)))
    .await;
    let request = server.request(Duration::from_secs(1)).unwrap();
    let request = request.to_ascii_lowercase();
    assert!(request.contains("authorization: bearer construction-secret"));
    assert!(!request.contains("openai-organization"));
    assert!(!request.contains("openai-project"));
    assert!(events.lock().unwrap().is_empty());
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
    let malformed_openai = OpenAI::new(malformed.base_url(), api_key, "model").unwrap();
    let unavailable_openai = OpenAI::new(unavailable.base_url(), api_key, "model").unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));

    async {
        tracing::info!("capture-probe");
        assert_eq!(
            malformed_openai
                .complete("prompt".to_string())
                .await
                .unwrap_err(),
            ai::Error::message("AI provider returned an invalid response")
        );
        assert_eq!(
            unavailable_openai
                .complete("prompt".to_string())
                .await
                .unwrap_err(),
            ai::Error::message("AI provider returned HTTP 500")
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
    let openai = OpenAI::new(server.base_url(), "secret", "model").unwrap();

    assert_eq!(
        openai.complete("prompt".to_string()).await.unwrap_err(),
        ai::Error::message(format!(
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
    let openai = OpenAI::new(server.base_url(), "secret", "model").unwrap();
    let expected = ai::Error::message(format!(
        "AI provider decoded response body exceeds the {}-byte limit",
        transport::MAX_BODY_BYTES
    ));

    assert_eq!(
        openai.complete("prompt".to_string()).await.unwrap_err(),
        expected
    );
    assert_eq!(
        openai.complete("prompt".to_string()).await.unwrap_err(),
        expected
    );
    assert_eq!(server.request_count(), 2);
}
