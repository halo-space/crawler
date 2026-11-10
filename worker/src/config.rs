use std::path::PathBuf;

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
