use serde_json::Value;

use crate::config::Error;
use crate::net::{ProxyConfig, TlsConfig};

pub(super) fn check_proxy(node: &str, value: Option<&Value>) -> Result<(), Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let object = value.as_object().ok_or_else(|| {
        Error::Message(format!(
            "node {node} request.proxy must be an object or null"
        ))
    })?;
    if let Some(name) = object.keys().find(|name| name.as_str() != "url") {
        return Err(Error::Message(format!(
            "node {node} request.proxy contains unsupported field: {name}"
        )));
    }
    let url = object
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Message(format!("node {node} request.proxy.url is required")))?;
    validate_proxy_url(url)
        .map_err(|message| Error::Message(format!("node {node} request.proxy.url {message}")))?;
    Ok(())
}

pub(super) fn check_tls(node: &str, value: Option<&Value>) -> Result<(), Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let object = value.as_object().ok_or_else(|| {
        Error::Message(format!("node {node} request.tls must be an object or null"))
    })?;
    if let Some(name) = object.keys().find(|name| name.as_str() != "verify") {
        return Err(Error::Message(format!(
            "node {node} request.tls contains unsupported field: {name}"
        )));
    }
    if let Some(verify) = object.get("verify")
        && !verify.is_boolean()
    {
        return Err(Error::Message(format!(
            "node {node} request.tls.verify must be a boolean"
        )));
    }
    Ok(())
}

pub(super) fn proxy(value: Option<&Value>) -> Result<Option<ProxyConfig>, Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::Message("request.proxy must be an object or null".to_string()))?;
    let url = object
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Message("request.proxy.url is required".to_string()))?;
    validate_proxy_url(url)
        .map_err(|message| Error::Message(format!("request.proxy.url {message}")))?;
    Ok(Some(ProxyConfig {
        url: url.to_string(),
    }))
}

pub(super) fn validate_proxy_url(value: &str) -> Result<(), String> {
    let url = url::Url::parse(value).map_err(|error| format!("is invalid: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("uses unsupported protocol: {}", url.scheme()));
    }
    if !url.has_host() {
        return Err("must have a host".to_string());
    }
    Ok(())
}

pub(super) fn tls(value: Option<&Value>) -> Result<Option<TlsConfig>, Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::Message("request.tls must be an object or null".to_string()))?;
    let accept_invalid_certs = object
        .get("verify")
        .and_then(Value::as_bool)
        .map(|verify| !verify)
        .unwrap_or(false);
    Ok(Some(TlsConfig {
        accept_invalid_certs,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_proxy_url_during_config_check() {
        let value = serde_json::json!({"url": "not a URL"});

        let error = check_proxy("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("request.proxy.url is invalid"));
    }

    #[test]
    fn rejects_unsupported_proxy_protocol() {
        let value = serde_json::json!({"url": "ftp://proxy.example.com"});

        let error = check_proxy("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("unsupported protocol: ftp"));
    }

    #[test]
    fn rejects_non_boolean_tls_verify_during_config_check() {
        let value = serde_json::json!({"verify": "false"});

        let error = check_tls("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("must be a boolean"));
    }

    #[test]
    fn rejects_unknown_transport_fields() {
        let proxy = serde_json::json!({"url": "http://localhost:8080", "auth": "secret"});
        let tls = serde_json::json!({"verify": true, "version": "1.3"});

        assert!(check_proxy("detail", Some(&proxy)).is_err());
        assert!(check_tls("detail", Some(&tls)).is_err());
    }
}
