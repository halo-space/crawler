use serde::{Deserialize, Deserializer, Serialize};

use crate::selector;

mod reference;
mod score;

const DEFAULT_MIN: f64 = 0.8;

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Config {
    min: f64,
}

impl Config {
    pub fn new(min: f64) -> Result<Self, selector::Error> {
        if !min.is_finite() || !(0.0..=1.0).contains(&min) {
            return Err(selector::Error::Css(
                "healing min must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(Self { min })
    }

    pub fn min(&self) -> f64 {
        self.min
    }
}

impl Default for Config {
    fn default() -> Self {
        Self { min: DEFAULT_MIN }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_min")]
    min: f64,
}

impl<'de> Deserialize<'de> for Config {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawConfig::deserialize(deserializer)?;
        Self::new(raw.min).map_err(serde::de::Error::custom)
    }
}

const fn default_min() -> f64 {
    DEFAULT_MIN
}

pub(super) fn recover<'a>(
    soup: &'a scrape_core::Soup,
    compiled: &scrape_core::CompiledSelector,
    config: &Config,
) -> Result<Vec<scrape_core::Tag<'a>>, selector::Error> {
    let reference = reference::Reference::new(compiled)?;
    score::select(soup, &reference, config.min)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select<'a>(
        soup: &'a scrape_core::Soup,
        expr: &str,
        config: &Config,
    ) -> Result<Vec<scrape_core::Tag<'a>>, selector::Error> {
        crate::selector::css::select(
            soup,
            expr,
            &crate::selector::css::Config::default().with_healing(*config),
        )
    }

    #[test]
    fn validates_min() {
        assert!(Config::new(0.0).is_ok());
        assert!(Config::new(1.0).is_ok());
        assert!(Config::new(-0.1).is_err());
        assert!(Config::new(1.1).is_err());
        assert_eq!(Config::default().min(), 0.8);
    }

    #[test]
    fn rejects_unknown_config_fields() {
        let error = serde_json::from_value::<Config>(serde_json::json!({
            "min": 0.8,
            "weight": 2
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn exact_match_wins_before_healing() {
        let soup =
            scrape_core::Soup::parse("<h2 class='title'>Exact</h2><h2 class='titles'>Similar</h2>");
        let nodes = select(&soup, "h2.title", &Config::default()).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].text(), "Exact");
    }

    #[test]
    fn heals_to_all_tied_highest_score_nodes_in_dom_order() {
        let soup = scrape_core::Soup::parse(
            "<article class='products'><h2 class='titles'>One</h2></article>\
             <article class='products'><h2 class='titles'>Two</h2></article>\
             <aside><h2 class='other'>Ignore</h2></aside>",
        );
        let nodes = select(
            &soup,
            "article.product > h2.title",
            &Config::new(0.7).unwrap(),
        )
        .unwrap();
        assert_eq!(
            nodes.iter().map(|node| node.text()).collect::<Vec<_>>(),
            ["One", "Two"]
        );
    }

    #[test]
    fn returns_empty_below_min() {
        let soup = scrape_core::Soup::parse("<div class='other'>No</div>");
        let nodes = select(&soup, "article#target.title", &Config::new(0.95).unwrap()).unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn invalid_css_does_not_enter_healing() {
        let soup = scrape_core::Soup::parse("<div></div>");
        assert!(select(&soup, "div[", &Config::default()).is_err());
    }

    #[test]
    fn heals_selector_lists_and_attribute_operators() {
        let soup = scrape_core::Soup::parse(
            "<a class='entries' href='/articles/1'>One</a><a class='other' href='/other'>Other</a>",
        );
        let nodes = select(
            &soup,
            "button.missing, a.entry[href^='/article/']",
            &Config::new(0.75).unwrap(),
        )
        .unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].text(), "One");
    }

    #[test]
    fn structural_pseudo_classes_contribute_to_score() {
        let soup = scrape_core::Soup::parse(
            "<ul><li class='items'>First</li><li class='items'>Second</li></ul>",
        );
        let nodes = select(&soup, "li.item:first-child", &Config::new(0.75).unwrap()).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].text(), "First");
    }

    #[test]
    fn not_is_scored_without_mode_fallback() {
        let soup = scrape_core::Soup::parse(
            "<section class='panels'>Visible</section>\
             <section class='panels hidden'>Hidden</section>",
        );
        let nodes = select(
            &soup,
            "section.panel:not(.hidden)",
            &Config::new(0.7).unwrap(),
        )
        .unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].text(), "Visible");
    }
}
