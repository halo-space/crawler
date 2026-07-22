use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Parse {
    #[serde(default)]
    pub fields: IndexMap<String, Field>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(default)]
    pub extractors: Vec<Extractor>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Extractor {
    Css {
        expr: String,
        #[serde(default)]
        args: crate::selector::css::Config,
    },
    Regex {
        expr: String,
        #[serde(default)]
        args: (),
    },
    Ai {
        expr: String,
        args: crate::selector::ai::Config,
    },
}

impl Extractor {
    pub fn css(expr: impl Into<String>, args: crate::selector::css::Config) -> Self {
        Self::Css {
            expr: expr.into(),
            args,
        }
    }

    pub fn regex(expr: impl Into<String>) -> Self {
        Self::Regex {
            expr: expr.into(),
            args: (),
        }
    }

    pub fn ai(expr: impl Into<String>, args: crate::selector::ai::Config) -> Self {
        Self::Ai {
            expr: expr.into(),
            args,
        }
    }

    pub fn expr(&self) -> &str {
        match self {
            Self::Css { expr, .. } | Self::Regex { expr, .. } | Self::Ai { expr, .. } => expr,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CssOutput<'a> {
    Element,
    Text,
    Attribute(&'a str),
}

pub(crate) fn parse_css_output(expr: &str) -> Result<(&str, CssOutput<'_>), String> {
    let expr = expr.trim();
    if let Some(output) = output_start(expr) {
        let suffix = &expr[output..];
        if suffix == "::text" {
            return require_css(&expr[..output], expr).map(|selector| (selector, CssOutput::Text));
        }
        if let Some(attr) = suffix
            .strip_prefix("::attr(")
            .and_then(|attr| attr.strip_suffix(')'))
        {
            let attr = attr.trim();
            if attr.is_empty() {
                return Err(format!("{expr}: attribute name cannot be empty"));
            }
            return require_css(&expr[..output], expr)
                .map(|selector| (selector, CssOutput::Attribute(attr)));
        }
        if suffix.starts_with("::attr(") {
            return Err(format!("{expr}: malformed attribute output"));
        }
    }
    require_css(expr, expr).map(|selector| (selector, CssOutput::Element))
}

fn output_start(expr: &str) -> Option<usize> {
    let bytes = expr.as_bytes();
    let mut index = 0;
    let mut quote = None;
    let mut brackets = 0_u32;
    let mut parentheses = 0_u32;
    let mut output = None;
    while index < bytes.len() {
        let value = bytes[index];
        if let Some(expected) = quote {
            match value {
                b'\\' => index += 1,
                value if value == expected => quote = None,
                _ => {}
            }
        } else {
            match value {
                b'\\' => index += 1,
                b'\'' | b'"' => quote = Some(value),
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    index += 2;
                    while index + 1 < bytes.len()
                        && (bytes[index] != b'*' || bytes[index + 1] != b'/')
                    {
                        index += 1;
                    }
                    index += usize::from(index + 1 < bytes.len());
                }
                b'[' => brackets += 1,
                b']' => brackets = brackets.saturating_sub(1),
                b'(' => parentheses += 1,
                b')' => parentheses = parentheses.saturating_sub(1),
                b':' if brackets == 0
                    && parentheses == 0
                    && bytes.get(index + 1) == Some(&b':') =>
                {
                    output = Some(index);
                    index += 1;
                }
                _ => {}
            }
        }
        index += 1;
    }
    output
}

fn require_css<'a>(selector: &'a str, expr: &str) -> Result<&'a str, String> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Err(format!("{expr}: selector cannot be empty"));
    }
    Ok(selector)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Bind {
    Pipeline {
        from: String,
        #[serde(default)]
        transforms: Vec<Transform>,
    },
    Template {
        template: String,
        vars: IndexMap<String, ValueRef>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Transform {
    pub kind: String,
    #[serde(flatten)]
    pub args: Map<String, Value>,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValueRef {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub value: Option<Value>,
}

impl<'de> Deserialize<'de> for ValueRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if let Value::Object(object) = &value
            && (object.contains_key("from") || object.contains_key("value"))
        {
            let reference =
                serde_json::from_value::<Reference>(value).map_err(serde::de::Error::custom)?;
            return Ok(Self {
                from: reference.from,
                value: reference.value,
            });
        }
        Ok(Self {
            from: None,
            value: Some(value),
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    value: Option<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn css_output_ignores_marker_text_inside_selectors() {
        assert_eq!(
            parse_css_output(r#"a[data-value='::attr(fake)']"#).unwrap(),
            (r#"a[data-value='::attr(fake)']"#, CssOutput::Element)
        );
        assert_eq!(
            parse_css_output(r#"a[data-value='::attr(fake)']::attr(href)"#).unwrap(),
            (
                r#"a[data-value='::attr(fake)']"#,
                CssOutput::Attribute("href")
            )
        );
        assert_eq!(
            parse_css_output(r#"a:not([data-value=")::text"])::text"#).unwrap(),
            (r#"a:not([data-value=")::text"])"#, CssOutput::Text)
        );
    }

    #[test]
    fn css_output_rejects_only_malformed_outer_suffix() {
        assert!(parse_css_output("article::attr(href").is_err());
        assert!(parse_css_output("article::attr()").is_err());
    }
}
