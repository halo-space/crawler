use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use thirtyfour::Proxy;
use thirtyfour::cdp::events::ResponseReceived;
use thirtyfour::prelude::{
    By, CapabilitiesHelper, ChromiumLikeCapabilities, DesiredCapabilities, ElementQueryable,
    WebDriver,
};
use tokio::sync::{Mutex, RwLock};
use url::Url;

use crate::{downloader, net};

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_MAX_SESSIONS: usize = 64;
const SELECTOR_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_STATUS: u16 = 200;

struct Session {
    driver: Arc<WebDriver>,
    serial: Mutex<()>,
    last_used: Mutex<Instant>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SessionKey {
    task_id: String,
    trace_id: String,
    origin: String,
    proxy: String,
    tls: bool,
    webdriver_url: Option<String>,
    binary: Option<String>,
}

pub struct Browser {
    webdriver_url: Option<String>,
    binary: Option<String>,
    idle_timeout: Duration,
    max_sessions: usize,
    sessions: RwLock<HashMap<SessionKey, Arc<Session>>>,
    opened: Mutex<bool>,
}

impl Default for Browser {
    fn default() -> Self {
        Self {
            webdriver_url: None,
            binary: None,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_sessions: DEFAULT_MAX_SESSIONS,
            sessions: RwLock::new(HashMap::new()),
            opened: Mutex::new(false),
        }
    }
}

impl Browser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_webdriver_url(mut self, url: impl Into<String>) -> Self {
        self.webdriver_url = Some(url.into());
        self
    }

    pub fn with_binary(mut self, path: impl Into<String>) -> Self {
        self.binary = Some(path.into());
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Result<Self, downloader::Error> {
        if timeout.is_zero() {
            return Err(downloader::Error::InvalidConfig(
                "browser session idle timeout must be positive".to_string(),
            ));
        }
        self.idle_timeout = timeout;
        Ok(self)
    }

    pub fn with_max_sessions(mut self, max_sessions: usize) -> Result<Self, downloader::Error> {
        if max_sessions == 0 {
            return Err(downloader::Error::InvalidConfig(
                "browser max sessions must be positive".to_string(),
            ));
        }
        self.max_sessions = max_sessions;
        Ok(self)
    }

    async fn ensure_open(&self) -> Result<(), downloader::Error> {
        if !*self.opened.lock().await {
            return Err(downloader::Error::InvalidConfig(
                "Browser Downloader is not open".to_string(),
            ));
        }
        Ok(())
    }

    async fn session(
        &self,
        request: &net::Request,
    ) -> Result<(SessionKey, Arc<Session>), downloader::Error> {
        let key = SessionKey::from_request(
            request,
            self.webdriver_url.as_deref(),
            self.binary.as_deref(),
        )?;

        self.reap_idle().await;
        if let Some(session) = self.sessions.read().await.get(&key).cloned() {
            return Ok((key, session));
        }

        let driver = Arc::new(self.create_driver(request).await?);
        let session = Arc::new(Session {
            driver,
            serial: Mutex::new(()),
            last_used: Mutex::new(Instant::now()),
        });
        let mut sessions = self.sessions.write().await;
        if let Some(existing) = sessions.get(&key).cloned() {
            let _ = session.driver.as_ref().clone().quit().await;
            return Ok((key, existing));
        }
        if sessions.len() >= self.max_sessions {
            if let Some((old_key, old_session)) = sessions
                .iter()
                .filter_map(|(key, session)| {
                    session
                        .last_used
                        .try_lock()
                        .ok()
                        .map(|last| (key.clone(), session.clone(), *last))
                })
                .min_by_key(|(_, _, last)| *last)
                .map(|(key, session, _)| (key, session))
            {
                sessions.remove(&old_key);
                let _ = old_session.driver.as_ref().clone().quit().await;
            }
        }
        sessions.insert(key.clone(), session.clone());
        Ok((key, session))
    }

    async fn create_driver(&self, request: &net::Request) -> Result<WebDriver, downloader::Error> {
        let mut caps = DesiredCapabilities::chrome();
        caps.add_arg("--headless=new").map_err(driver_error)?;
        caps.add_arg("--disable-gpu").map_err(driver_error)?;
        if let Some(binary) = &self.binary {
            caps.set_binary(binary).map_err(driver_error)?;
        }
        if request
            .tls
            .as_ref()
            .is_some_and(|tls| tls.accept_invalid_certs)
        {
            caps.accept_insecure_certs(true).map_err(driver_error)?;
        }
        if let Some(proxy) = &request.proxy {
            caps.set_proxy(Proxy::Manual {
                ftp_proxy: None,
                http_proxy: Some(proxy.url.clone()),
                ssl_proxy: Some(proxy.url.clone()),
                socks_proxy: None,
                socks_version: None,
                socks_username: None,
                socks_password: None,
                no_proxy: None,
            })
            .map_err(driver_error)?;
        }

        match &self.webdriver_url {
            Some(url) => WebDriver::new(url, caps).await.map_err(driver_error),
            None => WebDriver::managed(caps).await.map_err(driver_error),
        }
    }

    async fn fetch_in_session(
        &self,
        session: &Session,
        request: net::Request,
    ) -> Result<net::Response, downloader::Error> {
        if !matches!(request.method, net::Method::Get) {
            return Err(downloader::Error::InvalidConfig(format!(
                "Browser Downloader only supports GET, received {:?}",
                request.method
            )));
        }
        reject_sensitive_headers(&request)?;

        let timeout = Duration::from_millis(request.timeout.unwrap_or(60_000));
        let driver = session.driver.clone();
        tokio::time::timeout(timeout, async move {
            let cdp = driver.cdp();
            let cdp_session = cdp.connect().await.map_err(driver_error)?;
            let mut responses = cdp_session
                .subscribe::<ResponseReceived>()
                .await
                .map_err(driver_error)?;

            let url = Url::parse(&request.url)
                .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
            let mut headers = request
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_string(), value.to_string()))
                })
                .collect::<HashMap<_, _>>();
            let mut cookie_values = HashMap::new();
            let host = url.host_str().unwrap_or_default();
            for cookie in driver.get_all_cookies().await.map_err(driver_error)? {
                if cookie.domain.as_deref().is_none_or(|domain| {
                    let domain = domain.trim_start_matches('.');
                    host.eq_ignore_ascii_case(domain) || host.ends_with(&format!(".{domain}"))
                }) {
                    cookie_values.insert(cookie.name, cookie.value);
                }
            }
            if let Some(cookie) = request.cookies.request_header(&url).map_err(|error| {
                downloader::Error::InvalidConfig(format!("invalid Request cookies: {error}"))
            })? {
                if let Ok(value) = cookie.to_str() {
                    for pair in value.split(';') {
                        if let Some((name, value)) = pair.trim().split_once('=') {
                            cookie_values.insert(name.to_string(), value.to_string());
                        }
                    }
                }
            }
            if !cookie_values.is_empty() {
                let mut cookie_values = cookie_values.into_iter().collect::<Vec<_>>();
                cookie_values.sort_by(|left, right| left.0.cmp(&right.0));
                headers.insert(
                    "Cookie".to_string(),
                    cookie_values
                        .into_iter()
                        .map(|(name, value)| format!("{name}={value}"))
                        .collect::<Vec<_>>()
                        .join("; "),
                );
            }
            cdp.network()
                .set_extra_http_headers(headers)
                .await
                .map_err(driver_error)?;

            driver
                .goto(request.url.clone())
                .await
                .map_err(driver_error)?;

            if !request.wait_selector.is_empty() {
                driver
                    .query(By::Css(request.wait_selector.as_str()))
                    .wait(timeout, SELECTOR_POLL_INTERVAL)
                    .first()
                    .await
                    .map_err(driver_error)?;
            }

            let source = driver.source().await.map_err(driver_error)?;
            let body = source.into_bytes();
            let limit = request.max_body_bytes.unwrap_or(64 * 1024 * 1024);
            if body.len() as u64 > limit {
                return Err(downloader::Error::BodyTooLarge { limit });
            }

            let final_url = driver.current_url().await.map_err(driver_error)?;
            if !request.allows(&final_url) {
                let _ = cdp.network().set_extra_http_headers(HashMap::new()).await;
                return Err(downloader::Error::DisallowedRedirect(final_url.to_string()));
            }
            let mut status = DEFAULT_STATUS;
            let mut response_headers = net::Headers::new();
            let mut redirects = Vec::new();
            while let Ok(Some(event)) =
                tokio::time::timeout(Duration::from_millis(10), responses.next()).await
            {
                if event.r#type != thirtyfour::cdp::domains::network::ResourceType::Document {
                    continue;
                }
                if let Some(value) = event
                    .response
                    .get("url")
                    .and_then(serde_json::Value::as_str)
                {
                    let response_url = Url::parse(value)
                        .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
                    if !request.allows(&response_url) {
                        let _ = cdp.network().set_extra_http_headers(HashMap::new()).await;
                        return Err(downloader::Error::DisallowedRedirect(value.to_string()));
                    }
                    if response_url != url {
                        redirects.push(value.to_string());
                    }
                }
                if let Some(value) = event
                    .response
                    .get("status")
                    .and_then(serde_json::Value::as_u64)
                {
                    status = u16::try_from(value).unwrap_or(DEFAULT_STATUS);
                }
                if let Some(headers) = event
                    .response
                    .get("headers")
                    .and_then(serde_json::Value::as_object)
                {
                    for (name, value) in headers {
                        if let Some(value) = value.as_str() {
                            let _ = response_headers.try_set(name, value);
                        }
                    }
                }
            }

            let mut cookies = net::Cookies::new();
            let final_host = final_url.host_str().unwrap_or_default();
            for cookie in driver.get_all_cookies().await.map_err(driver_error)? {
                let applicable = cookie.domain.as_deref().is_none_or(|domain| {
                    let domain = domain.trim_start_matches('.');
                    final_host.eq_ignore_ascii_case(domain)
                        || final_host.ends_with(&format!(".{domain}"))
                });
                if applicable {
                    let _ = cookies.insert(&final_url, cookie.name, cookie.value);
                }
            }
            cdp.network()
                .set_extra_http_headers(HashMap::new())
                .await
                .map_err(driver_error)?;

            let mut response = net::Response::new(request, net::StatusCode(status), body);
            response.url = final_url.to_string();
            response.headers = response_headers;
            response.cookies = cookies;
            response.redirects = redirects;
            Ok(response)
        })
        .await
        .map_err(|_| downloader::Error::Timeout)?
    }

    async fn reap_idle(&self) {
        let now = Instant::now();
        let mut expired = Vec::new();
        {
            let sessions = self.sessions.read().await;
            for (key, session) in sessions.iter() {
                if session
                    .last_used
                    .try_lock()
                    .map(|last| now.duration_since(*last) >= self.idle_timeout)
                    .unwrap_or(false)
                {
                    expired.push((key.clone(), session.clone()));
                }
            }
        }
        if expired.is_empty() {
            return;
        }
        let mut sessions = self.sessions.write().await;
        for (key, session) in expired {
            if sessions.remove(&key).is_some() {
                let _ = session.driver.as_ref().clone().quit().await;
            }
        }
    }
}

impl downloader::Download for Browser {
    async fn open(&self) -> Result<(), downloader::Error> {
        let mut opened = self.opened.lock().await;
        if *opened {
            return Ok(());
        }
        if let Some(url) = &self.webdriver_url {
            Url::parse(url).map_err(|error| {
                downloader::Error::InvalidConfig(format!("invalid WebDriver URL: {error}"))
            })?;
        }
        *opened = true;
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        let mut opened = self.opened.lock().await;
        *opened = false;
        let sessions = std::mem::take(&mut *self.sessions.write().await);
        for session in sessions.into_values() {
            let _ = session.driver.as_ref().clone().quit().await;
        }
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        self.ensure_open().await?;
        if !matches!(request.method, net::Method::Get) {
            return Err(downloader::Error::InvalidConfig(format!(
                "Browser Downloader only supports GET, received {:?}",
                request.method
            )));
        }
        let (key, session) = self.session(&request).await?;
        let _serial = session.serial.lock().await;
        let result = self.fetch_in_session(&session, request).await;
        *session.last_used.lock().await = Instant::now();
        if result
            .as_ref()
            .is_err_and(|error| !matches!(error, downloader::Error::DisallowedRedirect(_)))
        {
            if let Some(session) = self.sessions.write().await.remove(&key) {
                let _ = session.driver.as_ref().clone().quit().await;
            }
        }
        result
    }
}

impl SessionKey {
    fn from_request(
        request: &net::Request,
        webdriver_url: Option<&str>,
        binary: Option<&str>,
    ) -> Result<Self, downloader::Error> {
        let url = Url::parse(&request.url)
            .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
        Ok(Self {
            task_id: request.task_id.clone(),
            trace_id: request.trace_id.clone(),
            origin: url.origin().ascii_serialization(),
            proxy: request
                .proxy
                .as_ref()
                .map(|proxy| proxy.url.clone())
                .unwrap_or_default(),
            tls: request
                .tls
                .as_ref()
                .is_some_and(|tls| tls.accept_invalid_certs),
            webdriver_url: webdriver_url.map(str::to_string),
            binary: binary.map(str::to_string),
        })
    }
}

fn reject_sensitive_headers(request: &net::Request) -> Result<(), downloader::Error> {
    for name in request.headers.as_map().keys() {
        if name == http::header::AUTHORIZATION || name == http::header::PROXY_AUTHORIZATION {
            return Err(downloader::Error::InvalidConfig(format!(
                "Browser Request header {name} is not supported"
            )));
        }
    }
    Ok(())
}

fn driver_error(error: impl std::fmt::Display) -> downloader::Error {
    downloader::Error::InvalidConfig(format!("Browser Downloader failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_uses_origin_not_path() {
        let mut first = net::Request::follow("https://example.com/index").unwrap();
        first.task_id = "task".to_string();
        first.trace_id = "trace".to_string();
        let mut second = net::Request::follow("https://example.com/detail").unwrap();
        second.task_id = first.task_id.clone();
        second.trace_id = first.trace_id.clone();

        assert_eq!(
            SessionKey::from_request(&first, None, None).unwrap(),
            SessionKey::from_request(&second, None, None).unwrap()
        );
    }
}
