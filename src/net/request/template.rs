use serde_json::Value;

use crate::config::Error;
use crate::utils::template::{self, Part};

pub(super) fn check(node: &str, name: &str, value: &Value) -> Result<(), Error> {
    match value {
        Value::String(value) => template::parse(value).map(|_| ()).map_err(|error| {
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
    let parts = template::parse(template).map_err(Error::Message)?;
    if let [Part::Variable(name)] = parts.as_slice() {
        return resolve(name).ok_or_else(|| {
            Error::Message(format!("request template variable is undefined: {}", name))
        });
    }

    let mut rendered = String::with_capacity(template.len());
    for part in parts {
        match part {
            Part::Text(text) => rendered.push_str(text),
            Part::Variable(name) => {
                let value = resolve(name).ok_or_else(|| {
                    Error::Message(format!("request template variable is undefined: {name}"))
                })?;
                rendered.push_str(&scalar(&value, "request template")?);
            }
        }
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
        Value::String(value) => references.extend(template::parse(value)?.into_iter().filter_map(
            |part| match part {
                Part::Variable(name) => Some(name),
                Part::Text(_) => None,
            },
        )),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_text_is_not_parsed_as_template_source() {
        let template = Value::String("{first}-{second}".to_string());
        let rendered = render(&template, &|name| match name {
            "first" => Some(Value::String("{second}".to_string())),
            "second" => Some(Value::String("value".to_string())),
            _ => None,
        })
        .unwrap();

        assert_eq!(rendered, Value::String("{second}-value".to_string()));
    }
}
