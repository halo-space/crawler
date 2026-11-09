use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Browser login configuration frozen into a Trace Snapshot with the Spider DSL.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub login_key: String,
    pub url: String,
    pub check: Check,
    #[serde(default)]
    pub actions: Vec<Action>,
    #[serde(default = "default_ttl")]
    pub ttl: u64,
    #[serde(default = "default_refresh_after")]
    pub refresh_after: u64,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginConfig")
            .field("login_key", &self.login_key)
            .field("url", &self.url)
            .field("check", &self.check)
            .field("actions_len", &self.actions.len())
            .field("ttl", &self.ttl)
            .field("refresh_after", &self.refresh_after)
            .finish()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Check {
    #[serde(default)]
    pub selector: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub cookie: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Action {
    #[serde(rename = "fill")]
    Fill { selector: String, value: String },
    #[serde(rename = "click")]
    Click { selector: String },
    #[serde(rename = "wait")]
    Wait {
        #[serde(default)]
        selector: String,
        #[serde(default)]
        url: String,
        timeout: u64,
    },
    #[serde(rename = "manual")]
    Manual { timeout: u64 },
}

impl fmt::Debug for Action {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fill { selector, .. } => formatter
                .debug_struct("Fill")
                .field("selector", selector)
                .field("value", &"<redacted>")
                .finish(),
            Self::Click { selector } => formatter
                .debug_struct("Click")
                .field("selector", selector)
                .finish(),
            Self::Wait {
                selector,
                url,
                timeout,
            } => formatter
                .debug_struct("Wait")
                .field("selector", selector)
                .field("url", url)
                .field("timeout", timeout)
                .finish(),
            Self::Manual { timeout } => formatter
                .debug_struct("Manual")
                .field("timeout", timeout)
                .finish(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.login_key.trim().is_empty() {
            return Err("spider.login.login_key must not be empty".to_string());
        }
        let url = url::Url::parse(&self.url)
            .map_err(|error| format!("spider.login.url is invalid: {error}"))?;
        if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
            return Err("spider.login.url must be an absolute HTTP(S) URL".to_string());
        }
        if self.check.selector.is_empty()
            && self.check.url.is_empty()
            && self.check.cookie.is_empty()
        {
            return Err("spider.login.check requires selector, url, or cookie".to_string());
        }
        if !self.check.selector.is_empty() {
            scrape_core::CompiledSelector::compile(&self.check.selector)
                .map_err(|error| format!("spider.login.check.selector is invalid: {error}"))?;
        }
        if self.ttl == 0 {
            return Err("spider.login.ttl must be positive".to_string());
        }
        if self.refresh_after == 0 || self.refresh_after > self.ttl {
            return Err(
                "spider.login.refresh_after must be positive and no greater than ttl".to_string(),
            );
        }
        for (index, action) in self.actions.iter().enumerate() {
            action.validate(index)?;
        }
        Ok(())
    }

    pub fn ttl_duration(&self) -> Duration {
        Duration::from_millis(self.ttl)
    }

    pub fn refresh_after_duration(&self) -> Duration {
        Duration::from_millis(self.refresh_after)
    }
}

impl Action {
    fn validate(&self, index: usize) -> Result<(), String> {
        let prefix = format!("spider.login.actions[{index}]");
        match self {
            Self::Fill { selector, value } => {
                if selector.trim().is_empty() || value.is_empty() {
                    return Err(format!("{prefix} fill requires selector and value"));
                }
                scrape_core::CompiledSelector::compile(selector)
                    .map_err(|error| format!("{prefix}.selector is invalid: {error}"))?;
                validate_value(value, &format!("{prefix}.value"))?;
            }
            Self::Click { selector } => {
                if selector.trim().is_empty() {
                    return Err(format!("{prefix} click requires selector"));
                }
                scrape_core::CompiledSelector::compile(selector)
                    .map_err(|error| format!("{prefix}.selector is invalid: {error}"))?;
            }
            Self::Wait {
                selector,
                url,
                timeout,
            } => {
                if selector.is_empty() == url.is_empty() {
                    return Err(format!(
                        "{prefix} wait requires exactly one of selector or url"
                    ));
                }
                if *timeout == 0 {
                    return Err(format!("{prefix}.timeout must be positive"));
                }
                if !selector.is_empty() {
                    scrape_core::CompiledSelector::compile(selector)
                        .map_err(|error| format!("{prefix}.selector is invalid: {error}"))?;
                }
            }
            Self::Manual { timeout } => {
                if *timeout == 0 {
                    return Err(format!("{prefix}.timeout must be positive"));
                }
            }
        }
        Ok(())
    }
}

fn validate_value(value: &str, field: &str) -> Result<(), String> {
    if let Some(name) = value.strip_prefix("env:")
        && (name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'))
    {
        return Err(format!("{field} has an invalid env reference"));
    }
    Ok(())
}

const fn default_ttl() -> u64 {
    86_400_000
}

const fn default_refresh_after() -> u64 {
    21_600_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_login_config_and_defaults_durations() {
        let config: Config = serde_yaml::from_str(
            r##"
login_key: jd_main
url: https://passport.jd.com/
check:
  selector: ".user-name"
actions:
  - kind: fill
    selector: "#loginname"
    value: env:JD_USERNAME
  - kind: manual
    timeout: 120000
"##,
        )
        .unwrap();

        config.validate().unwrap();
        assert_eq!(config.ttl_duration(), Duration::from_secs(86_400));
        assert_eq!(config.refresh_after_duration(), Duration::from_secs(21_600));
        let debug = format!("{config:?}");
        assert!(!debug.contains("JD_USERNAME"));
        assert!(!debug.contains("JD_PASSWORD"));
        assert!(debug.contains("actions_len: 2"));
    }

    #[test]
    fn rejects_missing_login_check() {
        let config: Config = serde_yaml::from_str(
            r#"
login_key: jd_main
url: https://passport.jd.com/
check: {}
"#,
        )
        .unwrap();

        assert!(config.validate().unwrap_err().contains("requires"));
    }
}
