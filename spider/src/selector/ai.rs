//! OpenAI-compatible selector that extracts JSON from the current Response.

use async_openai::Client as OpenAI;
use async_openai::config::OpenAIConfig;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs, ResponseFormat,
};
use serde_json::Value;
use std::time::Duration;
use tracing::instrument::WithSubscriber;

use crate::{net, selector};

const MAX_CONTENT_BYTES: usize = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Client {
    provider: OpenAI<OpenAIConfig>,
    base_url: String,
    model_name: String,
}

impl Client {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model_name: impl Into<String>,
    ) -> Result<Self, selector::Error> {
        let api_key = api_key.into();
        validate_api_key(&api_key)?;
        Self::build(base_url.into(), api_key, model_name.into())
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
        let api_key = std::env::var(&variable).map_err(|_| {
            selector::Error::Ai(format!(
                "api_key environment variable is not set: {variable}"
            ))
        })?;
        if api_key.trim().is_empty() {
            return Err(selector::Error::Ai(format!(
                "api_key environment variable is empty: {variable}"
            )));
        }
        validate_api_key(&api_key)?;
        Self::build(base_url.into(), api_key, model_name.into())
    }

    fn build(
        base_url: String,
        api_key: String,
        model_name: String,
    ) -> Result<Self, selector::Error> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let url = url::Url::parse(&base_url)
            .map_err(|_| selector::Error::Ai("invalid base_url".to_string()))?;
        if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
            return Err(selector::Error::Ai(
                "base_url must be an absolute http or https URL".to_string(),
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(selector::Error::Ai(
                "base_url must not contain credentials".to_string(),
            ));
        }
        if url.query().is_some() {
            return Err(selector::Error::Ai(
                "base_url must not contain a query".to_string(),
            ));
        }
        if url.fragment().is_some() {
            return Err(selector::Error::Ai(
                "base_url must not contain a fragment".to_string(),
            ));
        }
        if model_name.trim().is_empty() {
            return Err(selector::Error::Ai(
                "model_name cannot be empty".to_string(),
            ));
        }
        let config = OpenAIConfig::new()
            .with_org_id("")
            .with_project_id("")
            .with_api_base(&base_url)
            .with_api_key(&api_key);
        let http = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| selector::Error::Ai("failed to build AI HTTP client".to_string()))?;
        // Use the plain service so Engine error_parse remains the only retry policy.
        let provider = OpenAI::build(http.clone(), config)
            .with_http_service(async_openai::middleware::ReqwestService::new(http));
        Ok(Self {
            provider,
            base_url,
            model_name,
        })
    }

    pub(crate) async fn select(
        &self,
        response: &net::Response,
        expr: &str,
    ) -> Result<Value, selector::Error> {
        if response.body().len() > MAX_CONTENT_BYTES {
            return Err(selector::Error::Ai(format!(
                "response body exceeds the AI input limit of {MAX_CONTENT_BYTES} bytes"
            )));
        }
        let body = response
            .text()
            .map_err(|error| selector::Error::Ai(error.to_string()))?;
        let prompt = format!(
            "{expr}\n\n输出约束：只能返回一个合法 JSON 对象；禁止返回数组、标量、Markdown 代码块或说明文字。必须遵循提取要求中给出的字段结构。\n\n以下是需要提取的页面内容：\n<content>\n{body}\n</content>"
        );
        let message = ChatCompletionRequestUserMessageArgs::default()
            .content(prompt)
            .build()
            .map_err(|error| selector::Error::Ai(error.to_string()))?;
        let completion_request = CreateChatCompletionRequestArgs::default()
            .model(&self.model_name)
            .messages([message.into()])
            .response_format(ResponseFormat::JsonObject)
            .build()
            .map_err(|error| selector::Error::Ai(error.to_string()))?;
        let completion = self
            .provider
            .chat()
            .create(completion_request)
            // async-openai logs raw 4xx/5xx bodies before returning an error. Isolate only
            // this provider future so application tracing cannot receive secret response data.
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await
            .map_err(provider_error)?;
        let json = completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_deref())
            .ok_or_else(|| selector::Error::Ai("model response has no content".to_string()))?;
        let value: Value = serde_json::from_str(json).map_err(|error| {
            selector::Error::Ai(format!("model content is not valid JSON: {error}"))
        })?;
        if !value.is_object() {
            return Err(selector::Error::Ai(
                "model content must be a JSON object".to_string(),
            ));
        }
        Ok(value)
    }
}

fn validate_api_key(api_key: &str) -> Result<(), selector::Error> {
    if api_key.trim().is_empty() {
        return Err(selector::Error::Ai("api_key cannot be empty".to_string()));
    }
    let authorization = format!("Bearer {api_key}");
    authorization
        .parse::<reqwest::header::HeaderValue>()
        .map(|_| ())
        .map_err(|_| selector::Error::Ai("api_key is not a valid HTTP credential".to_string()))
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|char| char == '_' || char.is_ascii_alphabetic())
        && chars.all(|char| char == '_' || char.is_ascii_alphanumeric())
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("base_url", &display_base_url(&self.base_url))
            .field("model_name", &self.model_name)
            .finish_non_exhaustive()
    }
}

fn provider_error(error: OpenAIError) -> selector::Error {
    let message = match error {
        OpenAIError::Reqwest(error) if error.is_timeout() => "AI provider request timed out",
        OpenAIError::Reqwest(error) if error.is_connect() => "AI provider connection failed",
        OpenAIError::ApiError(error) => {
            return selector::Error::Ai(format!(
                "AI provider returned HTTP {}",
                error.status_code.as_u16()
            ));
        }
        OpenAIError::JSONDeserialize(..) => "AI provider returned an invalid response",
        _ => "AI provider request failed",
    };
    selector::Error::Ai(message.to_string())
}

fn display_base_url(base_url: &str) -> String {
    let Ok(mut url) = url::Url::parse(base_url) else {
        return "<invalid>".to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string().trim_end_matches('/').to_string()
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests;
