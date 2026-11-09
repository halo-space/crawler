use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
pub use thirtyfour::Cookie;
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
const DEFAULT_USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
];

struct Session {
    driver: Arc<WebDriver>,
    serial: Mutex<()>,
    last_used: Mutex<Instant>,
    authenticated: Mutex<HashMap<String, Instant>>,
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
    login_key: Option<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CookieKey {
    login_key: String,
    origin: String,
}

struct StoredCookies {
    saved_at: Instant,
    ttl: Duration,
    refresh_after: Duration,
    cookies: Vec<Cookie>,
}

pub type CookieLoadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Vec<Cookie>>, downloader::Error>> + Send + 'a>>;
pub type CookieSaveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), downloader::Error>> + Send + 'a>>;

pub trait CookieStore: Send + Sync + 'static {
    fn load<'a>(&'a self, login_key: &'a str, origin: &'a str) -> CookieLoadFuture<'a>;

    fn save<'a>(
        &'a self,
        login_key: &'a str,
        origin: &'a str,
        cookies: Vec<Cookie>,
        ttl: Duration,
        refresh_after: Duration,
    ) -> CookieSaveFuture<'a>;
}

#[derive(Default)]
pub struct MemoryCookieStore {
    values: Mutex<HashMap<CookieKey, StoredCookies>>,
}

impl MemoryCookieStore {
    async fn load_key(&self, key: &CookieKey) -> Option<Vec<Cookie>> {
        let mut values = self.values.lock().await;
        let expired = values
            .get(key)
            .is_some_and(|value| value.saved_at.elapsed() >= value.ttl);
        if expired {
            values.remove(key);
            return None;
        }
        let refresh = values
            .get(key)
            .is_some_and(|value| value.saved_at.elapsed() >= value.refresh_after);
        if refresh {
            return None;
        }
        values.get(key).map(|value| value.cookies.clone())
    }

    async fn save_key(
        &self,
        key: CookieKey,
        cookies: Vec<Cookie>,
        ttl: Duration,
        refresh_after: Duration,
    ) {
        self.values.lock().await.insert(
            key,
            StoredCookies {
                saved_at: Instant::now(),
                ttl,
                refresh_after,
                cookies,
            },
        );
    }
}

impl CookieStore for MemoryCookieStore {
    fn load<'a>(&'a self, login_key: &'a str, origin: &'a str) -> CookieLoadFuture<'a> {
        Box::pin(async move {
            Ok(self
                .load_key(&CookieKey {
                    login_key: login_key.to_string(),
                    origin: origin.to_string(),
                })
                .await)
        })
    }

    fn save<'a>(
        &'a self,
        login_key: &'a str,
        origin: &'a str,
        cookies: Vec<Cookie>,
        ttl: Duration,
        refresh_after: Duration,
    ) -> CookieSaveFuture<'a> {
        Box::pin(async move {
            self.save_key(
                CookieKey {
                    login_key: login_key.to_string(),
                    origin: origin.to_string(),
                },
                cookies,
                ttl,
                refresh_after,
            )
            .await;
            Ok(())
        })
    }
}

pub struct Browser<C = MemoryCookieStore> {
    webdriver_url: Option<String>,
    binary: Option<String>,
    headless: bool,
    idle_timeout: Duration,
    max_sessions: usize,
    sessions: RwLock<HashMap<SessionKey, Arc<Session>>>,
    cookies: Arc<C>,
    opened: Mutex<bool>,
}

impl Default for Browser<MemoryCookieStore> {
    fn default() -> Self {
        Self {
            webdriver_url: None,
            binary: None,
            headless: true,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_sessions: DEFAULT_MAX_SESSIONS,
            sessions: RwLock::new(HashMap::new()),
            cookies: Arc::new(MemoryCookieStore::default()),
            opened: Mutex::new(false),
        }
    }
}

impl Browser<MemoryCookieStore> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<C: CookieStore> Browser<C> {
    pub fn with_cookie_store<Next>(self, cookie_store: Next) -> Browser<Next>
    where
        Next: CookieStore,
    {
        Browser {
            webdriver_url: self.webdriver_url,
            binary: self.binary,
            headless: self.headless,
            idle_timeout: self.idle_timeout,
            max_sessions: self.max_sessions,
            sessions: self.sessions,
            cookies: Arc::new(cookie_store),
            opened: self.opened,
        }
    }

    pub fn with_webdriver_url(mut self, url: impl Into<String>) -> Self {
        self.webdriver_url = Some(url.into());
        self
    }

    pub fn with_binary(mut self, path: impl Into<String>) -> Self {
        self.binary = Some(path.into());
        self
    }

    pub fn with_headless(mut self, headless: bool) -> Self {
        self.headless = headless;
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
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(&key).cloned() {
                *session.last_used.lock().await = Instant::now();
                return Ok((key, session));
            }
        }

        let driver = Arc::new(self.create_driver(request).await?);
        let session = Arc::new(Session {
            driver,
            serial: Mutex::new(()),
            last_used: Mutex::new(Instant::now()),
            authenticated: Mutex::new(HashMap::new()),
        });
        let mut sessions = self.sessions.write().await;
        if let Some(existing) = sessions.get(&key).cloned() {
            let _ = session.driver.as_ref().clone().quit().await;
            return Ok((key, existing));
        }
        if sessions.len() >= self.max_sessions {
            let candidate = sessions
                .iter()
                .filter_map(|(key, session)| {
                    let serial = session.serial.try_lock().ok()?;
                    let last = *session.last_used.try_lock().ok()?;
                    drop(serial);
                    Some((key.clone(), session.clone(), last))
                })
                .min_by_key(|(_, _, last)| *last);
            if let Some((old_key, old_session, old_last)) = candidate
                && let Ok(_serial) = old_session.serial.try_lock()
            {
                let current_last = *old_session.last_used.lock().await;
                if current_last == old_last && sessions.remove(&old_key).is_some() {
                    let _ = old_session.driver.as_ref().clone().quit().await;
                }
            }
        }
        sessions.insert(key.clone(), session.clone());
        Ok((key, session))
    }

    async fn create_driver(&self, request: &net::Request) -> Result<WebDriver, downloader::Error> {
        let mut caps = DesiredCapabilities::chrome();
        if self.headless {
            caps.add_arg("--headless=new").map_err(driver_error)?;
        }
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
        let fetch_driver = driver.clone();
        let login = request
            .snapshot()
            .and_then(|snapshot| snapshot.dsl.as_ref())
            .and_then(|config| config.spider.login.clone());
        let cookies = Arc::clone(&self.cookies);
        let authenticated = &session.authenticated;
        let restore_state = Arc::new(Mutex::new(None::<(Vec<String>, Vec<Cookie>)>));
        let restore_state_inner = Arc::clone(&restore_state);
        let result = tokio::time::timeout(timeout, async move {
            if let Some(login) = login.as_ref() {
                ensure_login(&fetch_driver, authenticated, cookies.as_ref(), login).await?;
            }
            let url = Url::parse(&request.url)
                .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
            let request_cookies = request_cookie_values(&request, &url)?;
            if !request_cookies.is_empty() {
                fetch_driver
                    .goto(request.url.clone())
                    .await
                    .map_err(driver_error)?;
                let current_url = fetch_driver.current_url().await.map_err(driver_error)?;
                if current_url.origin() != url.origin() {
                    return Err(downloader::Error::InvalidConfig(
                        "Browser Request cookies cannot be applied after a cross-origin redirect"
                            .to_string(),
                    ));
                }
                let names = request_cookies
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                let host = url.host_str().unwrap_or_default();
                let previous = fetch_driver
                    .get_all_cookies()
                    .await
                    .map_err(driver_error)?
                    .into_iter()
                    .filter(|cookie| {
                        names.iter().any(|name| name == &cookie.name)
                            && cookie_applies_to_host(cookie, host)
                    })
                    .collect::<Vec<_>>();
                *restore_state_inner.lock().await = Some((names, previous));
                for (name, value) in request_cookies {
                    let mut cookie = Cookie::new(name, value);
                    cookie.set_path("/");
                    if url.scheme() == "https" {
                        cookie.set_secure(true);
                    }
                    fetch_driver
                        .add_cookie(cookie)
                        .await
                        .map_err(driver_error)?;
                }
            }
            let cdp = fetch_driver.cdp();
            let cdp_session = cdp.connect().await.map_err(driver_error)?;
            let mut responses = cdp_session
                .subscribe::<ResponseReceived>()
                .await
                .map_err(driver_error)?;

            let headers = browser_headers(&request);
            cdp.network()
                .set_extra_http_headers(headers)
                .await
                .map_err(driver_error)?;

            fetch_driver
                .goto(request.url.clone())
                .await
                .map_err(driver_error)?;

            if !request.wait_selector.is_empty() {
                fetch_driver
                    .query(By::Css(request.wait_selector.as_str()))
                    .wait(timeout, SELECTOR_POLL_INTERVAL)
                    .first()
                    .await
                    .map_err(driver_error)?;
            }

            let source = fetch_driver.source().await.map_err(driver_error)?;
            let body = source.into_bytes();
            let limit = request.max_body_bytes.unwrap_or(64 * 1024 * 1024);
            if body.len() as u64 > limit {
                return Err(downloader::Error::BodyTooLarge { limit });
            }

            let final_url = fetch_driver.current_url().await.map_err(driver_error)?;
            if !request.allows(&final_url) {
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
            for cookie in fetch_driver.get_all_cookies().await.map_err(driver_error)? {
                let applicable = cookie.domain.as_deref().is_none_or(|domain| {
                    let domain = domain.trim_start_matches('.');
                    final_host.eq_ignore_ascii_case(domain)
                        || final_host.ends_with(&format!(".{domain}"))
                });
                if applicable {
                    let _ = cookies.insert(&final_url, cookie.name, cookie.value);
                }
            }
            let mut response = net::Response::new(request, net::StatusCode(status), body);
            response.url = final_url.to_string();
            response.headers = response_headers;
            response.cookies = cookies;
            response.redirects = redirects;
            Ok(response)
        })
        .await
        .map_err(|_| downloader::Error::Timeout)?;

        let headers_result = driver
            .cdp()
            .network()
            .set_extra_http_headers(HashMap::new())
            .await
            .map_err(driver_error);
        let restore_result = if let Some((names, previous)) = restore_state.lock().await.take() {
            restore_browser_cookies(&driver, &names, &previous).await
        } else {
            Ok(())
        };
        match (result, headers_result, restore_result) {
            (Ok(response), Ok(()), Ok(())) => Ok(response),
            (Ok(_), Err(error), _) | (Ok(_), Ok(()), Err(error)) => Err(error),
            (Err(error), Err(cleanup), _) | (Err(error), Ok(()), Err(cleanup)) => {
                tracing::warn!(error = %cleanup, "Browser request cleanup failed");
                Err(error)
            }
            (Err(error), Ok(()), Ok(())) => Err(error),
        }
    }

    async fn reap_idle(&self) {
        let now = Instant::now();
        let mut sessions = self.sessions.write().await;
        let keys = sessions.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(session) = sessions.get(&key).cloned() else {
                continue;
            };
            let Ok(_serial) = session.serial.try_lock() else {
                continue;
            };
            let last = *session.last_used.lock().await;
            if now.duration_since(last) >= self.idle_timeout && sessions.remove(&key).is_some() {
                let _ = session.driver.as_ref().clone().quit().await;
            }
        }
    }
}

impl<C: CookieStore> downloader::Download for Browser<C> {
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
            let _serial = session.serial.lock().await;
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
            && let Some(session) = self.sessions.write().await.remove(&key)
        {
            let _ = session.driver.as_ref().clone().quit().await;
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
        let login_key = request
            .snapshot()
            .and_then(|snapshot| snapshot.dsl.as_ref())
            .and_then(|config| config.spider.login.as_ref())
            .map(|config| config.login_key.clone());
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
            login_key,
        })
    }
}

async fn ensure_login(
    driver: &WebDriver,
    authenticated: &Mutex<HashMap<String, Instant>>,
    cookies: &impl CookieStore,
    config: &crate::spider::login::Config,
) -> Result<(), downloader::Error> {
    let login_url = Url::parse(&config.url)
        .map_err(|error| downloader::Error::InvalidConfig(format!("invalid login URL: {error}")))?;
    let origin = login_url.origin().ascii_serialization();
    let key = format!("{}|{origin}", config.login_key);
    {
        let mut state = authenticated.lock().await;
        if state
            .get(&key)
            .is_some_and(|saved_at| saved_at.elapsed() < config.refresh_after_duration())
        {
            return Ok(());
        }
        state.remove(&key);
    }

    driver
        .goto(config.url.clone())
        .await
        .map_err(driver_error)?;
    if let Some(saved) = cookies.load(&config.login_key, &origin).await? {
        for cookie in saved {
            if cookie_applies_to_host(&cookie, login_url.host_str().unwrap_or_default()) {
                driver.add_cookie(cookie).await.map_err(driver_error)?;
            }
        }
        driver
            .goto(config.url.clone())
            .await
            .map_err(driver_error)?;
    } else {
        driver.delete_all_cookies().await.map_err(driver_error)?;
        driver
            .goto(config.url.clone())
            .await
            .map_err(driver_error)?;
    }

    if !login_check(driver, &config.check).await? {
        for action in &config.actions {
            run_login_action(driver, action).await?;
        }
        if !login_check(driver, &config.check).await? {
            return Err(downloader::Error::InvalidConfig(
                "Browser login check failed after login actions".to_string(),
            ));
        }
    }

    let current = driver
        .get_all_cookies()
        .await
        .map_err(driver_error)?
        .into_iter()
        .filter(|cookie| cookie_applies_to_host(cookie, login_url.host_str().unwrap_or_default()))
        .collect();
    cookies
        .save(
            &config.login_key,
            &origin,
            current,
            config.ttl_duration(),
            config.refresh_after_duration(),
        )
        .await?;
    authenticated.lock().await.insert(key, Instant::now());
    Ok(())
}

async fn login_check(
    driver: &WebDriver,
    check: &crate::spider::login::Check,
) -> Result<bool, downloader::Error> {
    if !check.selector.is_empty()
        && driver
            .query(By::Css(check.selector.as_str()))
            .first()
            .await
            .is_err()
    {
        return Ok(false);
    }
    if !check.url.is_empty() {
        let current = driver.current_url().await.map_err(driver_error)?;
        if !current.as_str().starts_with(&check.url) {
            return Ok(false);
        }
    }
    if !check.cookie.is_empty()
        && !driver
            .get_all_cookies()
            .await
            .map_err(driver_error)?
            .iter()
            .any(|cookie| cookie.name == check.cookie)
    {
        return Ok(false);
    }
    Ok(true)
}

async fn run_login_action(
    driver: &WebDriver,
    action: &crate::spider::login::Action,
) -> Result<(), downloader::Error> {
    match action {
        crate::spider::login::Action::Fill { selector, value } => {
            let value = resolve_login_value(value)?;
            let element = driver
                .query(By::Css(selector.as_str()))
                .first()
                .await
                .map_err(driver_error)?;
            element.clear().await.map_err(driver_error)?;
            element.send_keys(value).await.map_err(driver_error)?;
        }
        crate::spider::login::Action::Click { selector } => {
            driver
                .query(By::Css(selector.as_str()))
                .first()
                .await
                .map_err(driver_error)?
                .click()
                .await
                .map_err(driver_error)?;
        }
        crate::spider::login::Action::Wait {
            selector,
            url,
            timeout,
        } => {
            wait_login_condition(driver, selector, url, *timeout).await?;
        }
        crate::spider::login::Action::Manual { timeout } => {
            tokio::time::sleep(Duration::from_millis(*timeout)).await;
        }
    }
    Ok(())
}

async fn wait_login_condition(
    driver: &WebDriver,
    selector: &str,
    url: &str,
    timeout: u64,
) -> Result<(), downloader::Error> {
    let deadline = Instant::now() + Duration::from_millis(timeout);
    loop {
        if !selector.is_empty() && driver.query(By::Css(selector)).first().await.is_ok() {
            return Ok(());
        }
        if !url.is_empty()
            && driver
                .current_url()
                .await
                .map_err(driver_error)?
                .as_str()
                .starts_with(url)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(downloader::Error::Timeout);
        }
        tokio::time::sleep(SELECTOR_POLL_INTERVAL).await;
    }
}

fn resolve_login_value(value: &str) -> Result<String, downloader::Error> {
    let Some(name) = value.strip_prefix("env:") else {
        return Ok(value.to_string());
    };
    std::env::var(name).map_err(|_| {
        downloader::Error::InvalidConfig(format!(
            "login action references missing Worker environment variable: {name}"
        ))
    })
}

fn cookie_applies_to_host(cookie: &Cookie, host: &str) -> bool {
    cookie.domain.as_deref().is_none_or(|domain| {
        let domain = domain.trim_start_matches('.');
        host.eq_ignore_ascii_case(domain) || host.ends_with(&format!(".{domain}"))
    })
}

fn reject_sensitive_headers(request: &net::Request) -> Result<(), downloader::Error> {
    for name in request.headers.as_map().keys() {
        let safe = matches!(
            name,
            &http::header::ACCEPT
                | &http::header::ACCEPT_LANGUAGE
                | &http::header::CACHE_CONTROL
                | &http::header::IF_MODIFIED_SINCE
                | &http::header::IF_NONE_MATCH
                | &http::header::USER_AGENT
        );
        if !safe {
            return Err(downloader::Error::InvalidConfig(format!(
                "Browser Request header {name} is not supported because it could leak across a redirect"
            )));
        }
    }
    Ok(())
}

fn random_user_agent() -> &'static str {
    DEFAULT_USER_AGENTS[fastrand::usize(..DEFAULT_USER_AGENTS.len())]
}

fn browser_headers(request: &net::Request) -> HashMap<String, String> {
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
    if !headers.contains_key("user-agent") {
        headers.insert("user-agent".to_string(), random_user_agent().to_string());
    }
    headers
}

async fn restore_browser_cookies(
    driver: &WebDriver,
    names: &[String],
    previous: &[Cookie],
) -> Result<(), downloader::Error> {
    for name in names {
        driver.delete_cookie(name).await.map_err(driver_error)?;
    }
    for cookie in previous {
        let mut cookie = cookie.clone();
        if cookie
            .domain
            .as_deref()
            .is_some_and(|domain| domain.is_empty() || domain.contains(':'))
        {
            cookie.domain = None;
        }
        driver.add_cookie(cookie).await.map_err(driver_error)?;
    }
    Ok(())
}

fn request_cookie_values(
    request: &net::Request,
    url: &Url,
) -> Result<Vec<(String, String)>, downloader::Error> {
    let Some(header) = request.cookies.request_header(url).map_err(|error| {
        downloader::Error::InvalidConfig(format!("invalid Request cookies: {error}"))
    })?
    else {
        return Ok(Vec::new());
    };
    let value = header.to_str().map_err(|error| {
        downloader::Error::InvalidConfig(format!("invalid Request cookies: {error}"))
    })?;
    Ok(value
        .split(';')
        .filter_map(|pair| pair.trim().split_once('='))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect())
}

fn driver_error(error: impl std::fmt::Display) -> downloader::Error {
    downloader::Error::InvalidConfig(format!("Browser Downloader failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_uses_a_default_user_agent_only_when_request_does_not_specify_one() {
        let request = net::Request::follow("https://example.com").unwrap();
        let headers = browser_headers(&request);
        assert!(DEFAULT_USER_AGENTS.contains(&headers["user-agent"].as_str()));

        let mut request = request;
        request
            .headers
            .try_set("User-Agent", "custom-agent")
            .unwrap();
        let headers = browser_headers(&request);
        assert_eq!(headers["user-agent"], "custom-agent");
    }

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

    #[test]
    fn session_key_separates_trace_and_origin() {
        let mut first = net::Request::follow("https://example.com/index").unwrap();
        first.task_id = "task".to_string();
        first.trace_id = "trace-a".to_string();

        let mut different_trace = first.clone();
        different_trace.trace_id = "trace-b".to_string();

        let mut different_origin = first.clone();
        different_origin.url = "https://other.example.com/index".to_string();

        let first = SessionKey::from_request(&first, None, None).unwrap();
        let different_trace = SessionKey::from_request(&different_trace, None, None).unwrap();
        let different_origin = SessionKey::from_request(&different_origin, None, None).unwrap();

        assert_ne!(first, different_trace);
        assert_ne!(first, different_origin);
    }

    #[test]
    fn session_key_separates_login_keys_from_the_trace_snapshot() {
        let config = crate::config::Config::from_yaml(
            r#"
spider:
  name: books
  login:
    login_key: account_a
    url: https://example.com/login
    check:
      cookie: sid
  start:
    - node: index
      url: https://example.com
graph:
  nodes:
    index: {}
  edges: []
"#,
        )
        .unwrap();
        let snapshot = Arc::new(crate::trace::Snapshot::rules("task", config));
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.task_id = "task".to_string();
        request.trace_id = "trace".to_string();
        request.set_snapshot(snapshot);

        let key = SessionKey::from_request(&request, None, None).unwrap();

        assert_eq!(key.login_key.as_deref(), Some("account_a"));
    }
}
