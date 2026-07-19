use serde_json::Value;

use crate::config::Error;

pub(super) fn check(node: &str, name: &str, value: &Value) -> Result<(), Error> {
    match value {
        Value::String(value) => names(value).map(|_| ()).map_err(|error| {
            Error::Message(format!(
                "node {node} {name} has an invalid template: {error}"
            ))
        }),
        Value::Array(values) => {
            for value in values {
                check(node, name, value)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for value in values.values() {
                check(node, name, value)?;
            }
            Ok(())
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
    }
}

pub(super) fn render(
    value: &Value,
    resolve: &impl Fn(&str) -> Option<Value>,
) -> Result<Value, Error> {
    match value {
        Value::String(template) => render_string(template, resolve),
        Value::Array(values) => values
            .iter()
            .map(|value| render(value, resolve))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => values
            .iter()
            .map(|(name, value)| render(value, resolve).map(|value| (name.clone(), value)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(Value::Object),
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(value.clone()),
    }
}

pub(super) fn scalar(value: &Value, kind: &str) -> Result<String, Error> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(Error::Message(format!(
            "{kind} value must be a string, number, or boolean"
        ))),
    }
}

fn render_string(template: &str, resolve: &impl Fn(&str) -> Option<Value>) -> Result<Value, Error> {
    let names = names(template).map_err(Error::Message)?;
    if names.is_empty() {
        return Ok(Value::String(template.to_string()));
    }
    if names.len() == 1 && template == format!("{{{}}}", names[0]) {
        return resolve(names[0]).ok_or_else(|| {
            Error::Message(format!(
                "request template variable is undefined: {}",
                names[0]
            ))
        });
    }

    let mut rendered = template.to_string();
    for name in names {
        let value = resolve(name).ok_or_else(|| {
            Error::Message(format!("request template variable is undefined: {name}"))
        })?;
        rendered = rendered.replace(&format!("{{{name}}}"), &scalar(&value, "request template")?);
    }
    Ok(Value::String(rendered))
}

pub(crate) fn references(value: &Value) -> Result<Vec<&str>, String> {
    let mut references = Vec::new();
    collect(value, &mut references)?;
    Ok(references)
}

fn collect<'a>(value: &'a Value, references: &mut Vec<&'a str>) -> Result<(), String> {
    match value {
        Value::String(value) => references.extend(names(value)?),
        Value::Array(values) => {
            for value in values {
                collect(value, references)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect(value, references)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn names(template: &str) -> Result<Vec<&str>, String> {
    let mut names = Vec::new();
    let mut rest = template;
    while let Some(position) = rest.find(['{', '}']) {
        if rest.as_bytes()[position] == b'}' {
            return Err("closing brace has no matching opening brace".to_string());
        }
        let after_open = &rest[position + 1..];
        let close = after_open
            .find('}')
            .ok_or_else(|| "opening brace has no matching closing brace".to_string())?;
        let name = &after_open[..close];
        if name.is_empty()
            || name.contains('{')
            || !name
                .chars()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, '_' | '.'))
        {
            return Err(format!("invalid variable name: {name}"));
        }
        names.push(name);
        rest = &after_open[close + 1..];
    }
    Ok(names)
}
