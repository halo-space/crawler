//! OpenAI-compatible selector that extracts JSON from the current Response.

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{net, selector};

#[derive(Clone, Serialize)]
pub struct Config {
    base_url: String,
    api_key: ApiKey,
    model_name: String,
}

#[derive(Clone)]
enum ApiKey {
    Direct(String),
    Env(String),
}

impl Config {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model_name: impl Into<String>,
    ) -> Result<Self, selector::Error> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let api_key = api_key.into();
        let model_name = model_name.into();
        if api_key.trim().is_empty() {
            return Err(selector::Error::Ai("api_key cannot be empty".to_string()));
        }
        Self::build(base_url, ApiKey::Direct(api_key), model_name)
    }

    pub fn from_env(
        base_url: impl Into<String>,
        variable: impl Into<String>,
        model_name: impl Into<String>,
    ) -> Result<Self, selector::Error> {
        let variable = variable.into();
        if !valid_env_name(&variable) {
            return Err(selector::Error::Ai(
                "api_key environment variable name is invalid".to_string(),
            ));
        }
        Self::build(base_url.into(), ApiKey::Env(variable), model_name.into())
    }

    fn build(
        base_url: String,
        api_key: ApiKey,
        model_name: String,
    ) -> Result<Self, selector::Error> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let url = url::Url::parse(&base_url)
            .map_err(|error| selector::Error::Ai(format!("invalid base_url: {error}")))?;
        if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
            return Err(selector::Error::Ai(
                "base_url must be an absolute http or https URL".to_string(),
            ));
        }
        if model_name.trim().is_empty() {
            return Err(selector::Error::Ai(
                "model_name cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            base_url,
            api_key,
            model_name,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn model_name(&self) -> &str {
        &self.model_name
    }
}

impl ApiKey {
    fn resolve(&self) -> Result<String, selector::Error> {
        match self {
            Self::Direct(value) => Ok(value.clone()),
            Self::Env(variable) => std::env::var(variable)
                .map_err(|_| {
                    selector::Error::Ai(format!(
                        "api_key environment variable is not set: {variable}"
                    ))
                })
                .and_then(|value| {
                    if value.trim().is_empty() {
                        Err(selector::Error::Ai(format!(
                            "api_key environment variable is empty: {variable}"
                        )))
                    } else {
                        Ok(value)
                    }
                }),
        }
    }
}

impl Serialize for ApiKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Env(variable) => serializer.serialize_str(&format!("env:{variable}")),
            Self::Direct(_) => Err(serde::ser::Error::custom(
                "direct api keys cannot be persisted; use an env:VARIABLE reference",
            )),
        }
    }
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|char| char == '_' || char.is_ascii_alphabetic())
        && chars.all(|char| char == '_' || char.is_ascii_alphanumeric())
}

impl std::fmt::Debug for Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Config")
            .field("base_url", &self.base_url)
            .field("api_key", &"***")
            .field("model_name", &self.model_name)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    base_url: String,
    api_key: String,
    model_name: String,
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawConfig::deserialize(deserializer)?;
        let variable = raw.api_key.strip_prefix("env:").ok_or_else(|| {
            serde::de::Error::custom("api_key must use an env:VARIABLE reference")
        })?;
        Self::from_env(raw.base_url, variable, raw.model_name).map_err(serde::de::Error::custom)
    }
}

pub async fn select(
    response: &net::Response,
    expr: &str,
    config: &Config,
) -> Result<Value, selector::Error> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(selector::Error::Ai("prompt cannot be empty".to_string()));
    }
    let body = response
        .text()
        .map_err(|error| selector::Error::Ai(error.to_string()))?;
    let prompt = format!("{expr}\n\n以下是需要提取的页面内容：\n<content>\n{body}\n</content>");
    let message = ChatCompletionRequestUserMessageArgs::default()
        .content(prompt)
        .build()
        .map_err(|error| selector::Error::Ai(error.to_string()))?;
    let completion_request = CreateChatCompletionRequestArgs::default()
        .model(config.model_name())
        .messages([message.into()])
        .build()
        .map_err(|error| selector::Error::Ai(error.to_string()))?;
    let api_key = config.api_key.resolve()?;
    let client = Client::with_config(
        OpenAIConfig::new()
            .with_api_base(config.base_url())
            .with_api_key(&api_key),
    );
    let completion = client
        .chat()
        .create(completion_request)
        .await
        .map_err(|error| selector::Error::Ai(redact(error, &api_key)))?;
    let json = completion
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_deref())
        .ok_or_else(|| selector::Error::Ai("model response has no content".to_string()))?;
    serde_json::from_str(json)
        .map_err(|error| selector::Error::Ai(format!("model content is not valid JSON: {error}")))
}

fn redact(error: impl std::fmt::Display, secret: &str) -> String {
    error.to_string().replace(secret, "***")
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc::{Receiver, channel};

    pub fn server(content: Option<&str>) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let response = serde_json::json!({
            "id": "chatcmpl-test",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                },
                "finish_reason": "stop"
            }],
            "created": 0,
            "model": "test-model",
            "object": "chat.completion",
            "usage": null
        })
        .to_string();
        let (sender, receiver) = channel();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let _ = sender.send(request);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        });
        (format!("http://{address}"), receiver)
    }

    fn read_request(stream: &mut std::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
            })
            .unwrap_or_default();
        while bytes.len() < header_end + content_length {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&chunk[..count]);
        }
        String::from_utf8(bytes).unwrap()
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
        }
    }

    #[test]
    fn validates_config_and_hides_api_key_from_debug() {
        let config = Config::new("https://example.com/v1/", "secret", "model").unwrap();
        assert_eq!(config.base_url(), "https://example.com/v1");
        assert!(!format!("{config:?}").contains("secret"));
        assert_eq!(
            redact("provider echoed secret", "secret"),
            "provider echoed ***"
        );
        assert!(Config::new("not-a-url", "secret", "model").is_err());
        assert!(Config::new("https://example.com/v1", "", "model").is_err());
        assert!(Config::new("https://example.com/v1", "secret", "").is_err());
        assert!(serde_json::to_value(&config).is_err());

        let persisted =
            Config::from_env("https://example.com/v1", "OPENAI_API_KEY", "model").unwrap();
        assert_eq!(
            serde_json::to_value(&persisted).unwrap()["api_key"],
            Value::from("env:OPENAI_API_KEY")
        );
        assert!(Config::from_env("https://example.com/v1", "invalid-name", "model").is_err());
    }

    #[tokio::test]
    async fn extracts_json_and_sends_prompt_with_response_body() {
        let (base_url, request_receiver) = test_support::server(Some(r#"{"title":"Rust"}"#));
        let config = Config::new(base_url, "secret", "test-model").unwrap();
        let value = select(&response("<h1>Rust</h1>"), "提取标题并返回 JSON", &config)
            .await
            .unwrap();
        assert_eq!(value["title"], Value::from("Rust"));

        let raw_request = request_receiver.recv().unwrap();
        assert!(
            raw_request
                .to_ascii_lowercase()
                .contains("authorization: bearer secret")
        );
        let body = raw_request.split_once("\r\n\r\n").unwrap().1;
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["model"], Value::from("test-model"));
        let content = body["messages"][0]["content"].as_str().unwrap();
        assert!(content.contains("提取标题并返回 JSON"));
        assert!(content.contains("<h1>Rust</h1>"));
    }

    #[tokio::test]
    async fn rejects_missing_or_invalid_json_content() {
        let (base_url, _) = test_support::server(None);
        let config = Config::new(base_url, "secret", "test-model").unwrap();
        let error = select(&response("body"), "prompt", &config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no content"), "{error}");

        let (base_url, _) = test_support::server(Some("not-json"));
        let config = Config::new(base_url, "secret", "test-model").unwrap();
        let error = select(&response("body"), "prompt", &config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not valid JSON"));
    }
}
