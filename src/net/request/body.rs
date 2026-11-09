use serde_json::Value;

use crate::config::Error;
use crate::net::Body;

use super::template;

pub(super) fn check(node: &str, value: Option<&Value>) -> Result<(), Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let object = value.as_object().ok_or_else(|| {
        Error::Message(format!(
            "node {node} request.body must be an object or null"
        ))
    })?;
    if let Some(name) = object
        .keys()
        .find(|name| !matches!(name.as_str(), "kind" | "data"))
    {
        return Err(Error::Message(format!(
            "node {node} request.body contains unsupported field: {name}"
        )));
    }
    let kind = match object.get("kind") {
        Some(Value::String(kind)) => kind.as_str(),
        Some(_) => {
            return Err(Error::Message(format!(
                "node {node} request.body.kind must be a string"
            )));
        }
        None => "json",
    };
    if !matches!(kind, "json" | "text") {
        return Err(Error::Message(format!(
            "node {node} uses unsupported request.body kind: {kind}"
        )));
    }
    if kind == "text" && object.get("data").is_none_or(|data| !data.is_string()) {
        return Err(Error::Message(format!(
            "node {node} request.body text data is required and must be a string"
        )));
    }
    if let Some(data) = object.get("data") {
        template::check(node, "request.body.data", data)?;
    }
    Ok(())
}

pub(super) fn build(value: Option<&Value>) -> Result<Body, Error> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(Body::Empty);
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::Message("request.body must be an object or null".to_string()))?;
    let kind = object.get("kind").and_then(Value::as_str).unwrap_or("json");
    let data = object.get("data").cloned().unwrap_or(Value::Null);
    match kind {
        "json" => Ok(Body::Json(data)),
        "text" => Ok(Body::Text(
            data.as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| Error::Message("body.text value must be a string".to_string()))?,
        )),
        _ => Err(Error::Message(format!(
            "unsupported request body kind: {kind}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsupported_kind_during_config_check() {
        let value = serde_json::json!({"kind": "form", "data": {"name": "book"}});

        let error = check("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("unsupported request.body kind"));
    }

    #[test]
    fn rejects_unknown_fields_during_config_check() {
        let value = serde_json::json!({"kind": "json", "payload": {}});

        let error = check("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("unsupported field: payload"));
    }

    #[test]
    fn requires_text_data_during_config_check() {
        for value in [
            serde_json::json!({"kind": "text"}),
            serde_json::json!({"kind": "text", "data": null}),
            serde_json::json!({"kind": "text", "data": 1}),
        ] {
            let error = check("detail", Some(&value)).unwrap_err();

            assert!(error.to_string().contains("data is required"));
        }
    }

    #[test]
    fn accepts_string_text_data_during_config_check() {
        let value = serde_json::json!({"kind": "text", "data": "{fields.query}"});

        check("detail", Some(&value)).unwrap();
    }

    #[test]
    fn validates_nested_json_templates() {
        let value = serde_json::json!({
            "kind": "json",
            "data": {"title": "{fields.title"}
        });

        let error = check("detail", Some(&value)).unwrap_err();

        assert!(error.to_string().contains("invalid template"));
    }
}
