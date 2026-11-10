use std::path::PathBuf;

use crate::Error;

#[derive(Clone, Debug)]
pub struct Config {
    pub concurrency: usize,
    pub claim_limit: usize,
    pub store_dir: PathBuf,
    pub browser: BrowserConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 5,
            claim_limit: 5,
            store_dir: PathBuf::from("./data/items"),
            browser: BrowserConfig::default(),
        }
    }
}

impl Config {
    /// Loads only Worker-local settings. Scheduler identity and backend settings belong to the Scheduler.
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            concurrency: parse_positive("HARBOR_WORKER_CONCURRENCY", 5)?,
            claim_limit: parse_positive("HARBOR_WORKER_LIMIT", 5)?,
            store_dir: optional("HARBOR_ITEM_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./data/items")),
            browser: BrowserConfig::from_env()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BrowserConfig {
    pub webdriver_url: Option<String>,
    pub binary: Option<String>,
    pub headless: bool,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            webdriver_url: None,
            binary: None,
            headless: true,
        }
    }
}

impl BrowserConfig {
    fn from_env() -> Result<Self, Error> {
        Ok(Self {
            webdriver_url: optional("HARBOR_BROWSER_WEBDRIVER_URL"),
            binary: optional("HARBOR_BROWSER_BINARY"),
            headless: parse_bool("HARBOR_BROWSER_HEADLESS", true)?,
        })
    }
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_positive(name: &str, default: usize) -> Result<usize, Error> {
    let value = optional(name)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|error| Error::Config(format!("{name} must be a positive integer: {error}")))?
        .unwrap_or(default);
    if value == 0 {
        return Err(Error::Config(format!("{name} must be positive")));
    }
    Ok(value)
}

fn parse_bool(name: &str, default: bool) -> Result<bool, Error> {
    match optional(name).as_deref() {
        None => Ok(default),
        Some("1" | "true" | "TRUE" | "yes" | "YES") => Ok(true),
        Some("0" | "false" | "FALSE" | "no" | "NO") => Ok(false),
        Some(value) => Err(Error::Config(format!(
            "{name} must be true or false, received {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boolean_defaults() {
        assert!(parse_bool("MISSING_HARBOR_FLAG", true).unwrap());
    }

    #[test]
    fn rejects_zero_limits() {
        assert!(parse_positive("HARBOR_TEST_LIMIT", 0).is_err());
    }
}
