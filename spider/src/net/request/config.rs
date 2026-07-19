use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::config::Error;
use crate::middleware;
use crate::net::{Method, Mode, Request};

use super::{body, template, transport};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub download_mode: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub dont_filter: Option<bool>,
    #[serde(default)]
    pub headers: HashMap<String, Value>,
    #[serde(default)]
    pub cookies: HashMap<String, Value>,
    #[serde(default)]
    pub body: Option<Value>,
    #[serde(default)]
    pub proxy: Option<Value>,
    #[serde(default)]
    pub tls: Option<Value>,
    #[serde(default)]
    pub middlewares: Vec<middleware::Spec>,
}

impl Config {
    pub(crate) fn validate(&self, node: &str) -> Result<(), Error> {
        if let Some(protocol) = &self.protocol
            && !protocol.eq_ignore_ascii_case("http")
        {
            return Err(Error::Message(format!(
                "node {node} uses unsupported protocol: {protocol}"
            )));
        }
        if let Some(download_mode) = &self.download_mode
            && !matches!(
                download_mode.to_ascii_lowercase().as_str(),
                "http" | "browser"
            )
        {
            return Err(Error::Message(format!(
                "node {node} uses unsupported download_mode: {download_mode}"
            )));
        }
        if let Some(method) = &self.method {
            method.parse::<Method>().map_err(|_| {
                Error::Message(format!("node {node} uses unsupported method: {method}"))
            })?;
        }
        let mut header_names = HashSet::with_capacity(self.headers.len());
        for (name, value) in &self.headers {
            let header_name =
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                    Error::Message(format!(
                        "node {node} has an invalid header name {name}: {error}"
                    ))
                })?;
            if !header_names.insert(header_name) {
                return Err(Error::Message(format!(
                    "node {node} contains duplicate header name: {name}"
                )));
            }
            check_scalar(node, &format!("header {name}"), value)?;
            template::check(node, &format!("header {name}"), value)?;
            check_header_value(node, &format!("header {name}"), value)?;
        }
        for (name, value) in &self.cookies {
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                Error::Message(format!(
                    "node {node} has an invalid cookie name {name}: {error}"
                ))
            })?;
            check_scalar(node, &format!("cookie {name}"), value)?;
            template::check(node, &format!("cookie {name}"), value)?;
            check_header_value(node, &format!("cookie {name}"), value)?;
        }
        body::check(node, self.body.as_ref())?;
        transport::check_proxy(node, self.proxy.as_ref())?;
        transport::check_tls(node, self.tls.as_ref())?;
        for spec in &self.middlewares {
            middleware::check(spec).map_err(|error| {
                Error::Message(format!("node {node} has invalid middleware: {error}"))
            })?;
        }
        Ok(())
    }

    pub(crate) fn validate_with(
        &self,
        node: &str,
        valid: impl Fn(&str) -> bool,
    ) -> Result<(), Error> {
        self.validate(node)?;
        for value in self
            .headers
            .values()
            .chain(self.cookies.values())
            .chain(self.body.iter())
        {
            for reference in template::references(value).map_err(|error| {
                Error::Message(format!(
                    "node {node} has an invalid request template: {error}"
                ))
            })? {
                if !valid(reference) {
                    return Err(Error::Message(format!(
                        "node {node} request template contains undefined reference: {reference}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn apply_with(
        &self,
        request: &mut Request,
        resolve: impl Fn(&str) -> Option<Value>,
    ) -> Result<(), Error> {
        request.method = self
            .method
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|_| Error::Message("invalid request method".to_string()))?
            .unwrap_or_default();
        request.timeout = self.timeout;
        request.dont_filter = self.dont_filter.unwrap_or(false);
        if let Some(mode) = self.download_mode.as_deref() {
            request.mode = match mode.to_ascii_lowercase().as_str() {
                "http" => Mode::Http,
                "browser" => Mode::Browser,
                _ => return Err(Error::Message(format!("unsupported download mode: {mode}"))),
            };
        }

        for (key, value) in &self.headers {
            let value = template::render(value, &resolve)?;
            request.headers.insert(
                key.clone(),
                template::scalar(&value, &format!("header {key}"))?,
            );
        }
        for (key, value) in &self.cookies {
            let value = template::render(value, &resolve)?;
            request.cookies.insert(
                key.clone(),
                template::scalar(&value, &format!("cookie {key}"))?,
            );
        }

        request.body = self
            .body
            .as_ref()
            .map(|body| template::render(body, &resolve))
            .transpose()?
            .as_ref()
            .map_or_else(|| body::build(None), |value| body::build(Some(value)))?;
        request.proxy = transport::proxy(self.proxy.as_ref())?;
        request.tls = transport::tls(self.tls.as_ref())?;
        request.middlewares.clone_from(&self.middlewares);
        Ok(())
    }
}

fn check_scalar(node: &str, name: &str, value: &Value) -> Result<(), Error> {
    if matches!(value, Value::String(_) | Value::Bool(_) | Value::Number(_)) {
        Ok(())
    } else {
        Err(Error::Message(format!(
            "node {node} {name} must be a string, number, or boolean"
        )))
    }
}

fn check_header_value(node: &str, name: &str, value: &Value) -> Result<(), Error> {
    let value = template::render(value, &|_| Some(Value::String("value".to_string())))?;
    let value = template::scalar(&value, name)?;
    reqwest::header::HeaderValue::from_str(&value).map_err(|error| {
        Error::Message(format!("node {node} has an invalid {name} value: {error}"))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_fields_during_deserialization() {
        let error = serde_json::from_value::<Config>(serde_json::json!({
            "method": "GET",
            "user_agent": "crawler"
        }))
        .unwrap_err();

        assert!(error.to_string().contains("unknown field `user_agent`"));
    }

    #[test]
    fn rejects_invalid_literal_header_and_cookie_values() {
        let header = serde_json::from_value::<Config>(serde_json::json!({
            "headers": {"X-Test": "line one\nline two"}
        }))
        .unwrap();
        assert!(header.validate("index").is_err());

        let cookie = serde_json::from_value::<Config>(serde_json::json!({
            "cookies": {"session": "line one\nline two"}
        }))
        .unwrap();
        assert!(cookie.validate("index").is_err());
    }

    #[test]
    fn validates_header_templates_with_safe_placeholders() {
        let config = serde_json::from_value::<Config>(serde_json::json!({
            "headers": {"X-Trace": "prefix-{trace_id}"},
            "cookies": {"session": "{session_id}"}
        }))
        .unwrap();

        config.validate("index").unwrap();
    }

    #[test]
    fn rejects_case_insensitive_duplicate_header_names() {
        let config = serde_json::from_value::<Config>(serde_json::json!({
            "headers": {
                "X-Trace": "one",
                "x-trace": "two"
            }
        }))
        .unwrap();

        let error = config.validate("index").unwrap_err();

        assert!(error.to_string().contains("duplicate header name"));
    }
}
