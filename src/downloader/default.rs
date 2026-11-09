use crate::downloader::{self, browser::Browser, http::Http};
use crate::net::{self, Mode};

pub struct Downloader {
    http: Http,
    browser: Browser,
}

impl Downloader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_http(mut self, http: Http) -> Self {
        self.http = http;
        self
    }

    pub fn with_browser(mut self, browser: Browser) -> Self {
        self.browser = browser;
        self
    }
}

impl Default for Downloader {
    fn default() -> Self {
        Self {
            http: Http::new(),
            browser: Browser::default(),
        }
    }
}

impl downloader::Download for Downloader {
    async fn open(&self) -> Result<(), downloader::Error> {
        if let Err(error) = self.http.open().await {
            let _ = self.http.close().await;
            return Err(error);
        }
        if let Err(error) = self.browser.open().await {
            let _ = self.browser.close().await;
            let _ = self.http.close().await;
            return Err(error);
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        let browser_result = self.browser.close().await;
        let http_result = self.http.close().await;
        browser_result.and(http_result)
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        match request.mode {
            Mode::Http => self.http.fetch(request).await,
            Mode::Browser => self.browser.fetch(request).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::downloader::Download;

    #[tokio::test]
    async fn rejects_non_get_browser_requests_before_starting_a_session() {
        let request = net::Request::follow("https://example.com")
            .unwrap()
            .mode(Mode::Browser)
            .method(net::Method::Post);

        let downloader = Downloader::new();
        downloader.open().await.unwrap();
        let error = downloader.fetch(request).await.unwrap_err();

        assert!(
            matches!(error, downloader::Error::InvalidConfig(message) if message.contains("only supports GET"))
        );
    }
}
