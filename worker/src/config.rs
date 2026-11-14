use std::path::PathBuf;

use harbor::net::Mode;

#[derive(Clone, Debug)]
pub struct Config {
    pub concurrency: usize,
    pub claim_limit: usize,
    pub store_dir: PathBuf,
    pub download_modes: Vec<Mode>,
    pub browser: BrowserConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 5,
            claim_limit: 5,
            store_dir: PathBuf::from("./data/items"),
            download_modes: vec![Mode::Http],
            browser: BrowserConfig::default(),
        }
    }
}

impl Config {
    pub fn with_download_modes(mut self, modes: impl IntoIterator<Item = Mode>) -> Self {
        self.download_modes.clear();
        for mode in modes {
            if !self.download_modes.contains(&mode) {
                self.download_modes.push(mode);
            }
        }
        self
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_worker_supports_http_only() {
        assert_eq!(Config::default().download_modes, [Mode::Http]);
    }

    #[test]
    fn download_modes_are_explicit_and_unique() {
        let config =
            Config::default().with_download_modes([Mode::Browser, Mode::Http, Mode::Browser]);

        assert_eq!(config.download_modes, [Mode::Browser, Mode::Http]);
    }
}
