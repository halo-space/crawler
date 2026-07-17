use serde::{Deserialize, Deserializer, Serialize};

use crate::{net, selector};

pub mod healing;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Config {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    healing: Option<healing::Config>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    healing: Option<healing::Config>,
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Option::<RawConfig>::deserialize(deserializer)?.unwrap_or_default();
        Ok(Self {
            healing: raw.healing,
        })
    }
}

impl Config {
    pub fn with_healing(mut self, config: healing::Config) -> Self {
        self.healing = Some(config);
        self
    }

    pub fn healing(&self) -> Option<&healing::Config> {
        self.healing.as_ref()
    }
}

pub(crate) fn parse(response: &net::Response) -> Result<scrape_core::Soup, net::Error> {
    response.text().map(|html| scrape_core::Soup::parse(&html))
}

pub fn select<'a>(
    soup: &'a scrape_core::Soup,
    expr: &str,
    config: &Config,
) -> Result<Vec<scrape_core::Tag<'a>>, selector::Error> {
    let compiled = scrape_core::CompiledSelector::compile(expr)
        .map_err(|error| selector::Error::Css(error.to_string()))?;
    let exact = soup.select_compiled(&compiled);
    if !exact.is_empty() {
        return Ok(exact);
    }
    let Some(config) = config.healing() else {
        return Ok(exact);
    };
    healing::recover(soup, &compiled, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_selection_without_healing_returns_empty_on_miss() {
        let soup = scrape_core::Soup::parse("<h1 class='titles'>Title</h1>");
        let nodes = select(&soup, "h1.title", &Config::default()).unwrap();

        assert!(nodes.is_empty());
    }

    #[test]
    fn config_deserializes_healing_and_rejects_unknown_fields() {
        let config = serde_json::from_value::<Config>(serde_json::json!({
            "healing": {"min": 0.7}
        }))
        .unwrap();
        assert_eq!(config.healing().unwrap().min(), 0.7);
        assert!(
            serde_json::from_value::<Config>(serde_json::Value::Null)
                .unwrap()
                .healing()
                .is_none()
        );

        let error =
            serde_json::from_value::<Config>(serde_json::json!({"fallback": true})).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }
}
