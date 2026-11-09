use std::time::Duration;

use async_openai::Client as Provider;
use async_openai::config::Config as ProviderConfig;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs, ResponseFormat,
};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use secrecy::{ExposeSecret, SecretString};
use tracing::instrument::WithSubscriber;

use super::transport;
use crate::ai;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Reusable OpenAI-compatible provider selected for one Worker.
pub struct OpenAI {
    provider: Provider<Config>,
    base_url: String,
    model_name: String,
}

#[derive(Clone)]
struct Config {
    base_url: String,
    api_key: SecretString,
}

impl Config {
    fn new(base_url: String, api_key: String) -> Result<Self, ai::Error> {
        validate_base_url(&base_url)?;
        authorization(&api_key)?;
        Ok(Self {
            base_url,
            api_key: api_key.into(),
        })
    }
}

impl ProviderConfig for Config {
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            authorization(self.api_key.expose_secret())
                .expect("API key was validated when the provider was constructed"),
        );
        headers
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn query(&self) -> Vec<(&str, &str)> {
        Vec::new()
    }

    fn api_base(&self) -> &str {
        &self.base_url
    }

    fn api_key(&self) -> &SecretString {
        &self.api_key
    }
}

impl OpenAI {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model_name: impl Into<String>,
    ) -> Result<Self, ai::Error> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let api_key = api_key.into();
        let model_name = model_name.into();
        let config = Config::new(base_url.clone(), api_key)?;
        if model_name.trim().is_empty() {
            return Err(ai::Error::message("model_name cannot be empty"));
        }

        let http = reqwest::Client::builder()
            .retry(reqwest::retry::never())
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| ai::Error::message("failed to build AI HTTP client"))?;
        let provider =
            Provider::build(http.clone(), config).with_http_service(transport::Service::new(http));

        Ok(Self {
            provider,
            base_url,
            model_name,
        })
    }

    pub(crate) async fn complete(&self, prompt: String) -> Result<String, ai::Error> {
        let message = ChatCompletionRequestUserMessageArgs::default()
            .content(prompt)
            .build()
            .map_err(|error| ai::Error::message(error.to_string()))?;
        let request = CreateChatCompletionRequestArgs::default()
            .model(&self.model_name)
            .messages([message.into()])
            .response_format(ResponseFormat::JsonObject)
            .build()
            .map_err(|error| ai::Error::message(error.to_string()))?;
        let completion = self
            .provider
            .chat()
            .create(request)
            // async-openai logs raw 4xx/5xx bodies before returning an error. Isolate only
            // this provider future so application tracing cannot receive secret response data.
            .with_subscriber(tracing::subscriber::NoSubscriber::default())
            .await
            .map_err(provider_error)?;

        completion
            .choices
            .first()
            .and_then(|choice| choice.message.content.clone())
            .ok_or_else(|| ai::Error::message("model response has no content"))
    }
}

impl std::fmt::Debug for OpenAI {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAI")
            .field("base_url", &display_base_url(&self.base_url))
            .field("model_name", &self.model_name)
            .finish_non_exhaustive()
    }
}

fn validate_base_url(base_url: &str) -> Result<(), ai::Error> {
    let url = url::Url::parse(base_url).map_err(|_| ai::Error::message("invalid base_url"))?;
    if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
        return Err(ai::Error::message(
            "base_url must be an absolute http or https URL",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ai::Error::message("base_url must not contain credentials"));
    }
    if url.query().is_some() {
        return Err(ai::Error::message("base_url must not contain a query"));
    }
    if url.fragment().is_some() {
        return Err(ai::Error::message("base_url must not contain a fragment"));
    }
    Ok(())
}

fn authorization(api_key: &str) -> Result<HeaderValue, ai::Error> {
    if api_key.trim().is_empty() {
        return Err(ai::Error::message("api_key cannot be empty"));
    }
    let mut value: HeaderValue = format!("Bearer {api_key}")
        .parse()
        .map_err(|_| ai::Error::message("api_key is not a valid HTTP credential"))?;
    value.set_sensitive(true);
    Ok(value)
}

fn provider_error(error: OpenAIError) -> ai::Error {
    if let Some(limit) = transport::body_limit(&error) {
        return ai::Error::message(format!(
            "AI provider decoded response body exceeds the {limit}-byte limit"
        ));
    }
    let message = match error {
        OpenAIError::Reqwest(error) if error.is_timeout() => "AI provider request timed out",
        OpenAIError::Reqwest(error) if error.is_connect() => "AI provider connection failed",
        OpenAIError::ApiError(error) => {
            return ai::Error::message(format!(
                "AI provider returned HTTP {}",
                error.status_code.as_u16()
            ));
        }
        OpenAIError::JSONDeserialize(..) => "AI provider returned an invalid response",
        _ => "AI provider request failed",
    };
    ai::Error::message(message)
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
mod tests;
