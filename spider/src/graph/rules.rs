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
    if let Some(selector) = expr.strip_suffix("::text") {
        return require_css(selector, expr).map(|selector| (selector, CssOutput::Text));
    }
    if let Some((selector, attr)) = expr.rsplit_once("::attr(")
        && let Some(attr) = attr.strip_suffix(')')
    {
        let attr = attr.trim();
        if attr.is_empty() {
            return Err(format!("{expr}: attribute name cannot be empty"));
        }
        return require_css(selector, expr).map(|selector| (selector, CssOutput::Attribute(attr)));
    }
    if expr.contains("::attr(") {
        return Err(format!("{expr}: malformed attribute output"));
    }
    require_css(expr, expr).map(|selector| (selector, CssOutput::Element))
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
