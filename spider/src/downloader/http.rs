use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::{downloader, net};

const MAX_REDIRECTS: usize = 10;
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Key {
    proxy: Option<String>,
    accept_invalid_certs: bool,
}

impl From<&net::Request> for Key {
    fn from(request: &net::Request) -> Self {
        Self {
            proxy: request.proxy.as_ref().map(|proxy| proxy.url.clone()),
            accept_invalid_certs: request
                .tls
                .as_ref()
                .is_some_and(|tls| tls.accept_invalid_certs),
        }
    }
}

struct Client {
    inner: reqwest::Client,
    state: Mutex<State>,
}

impl Client {
    fn new(inner: reqwest::Client) -> Self {
        Self {
            inner,
            state: Mutex::new(State::default()),
        }
    }

    fn expired(&self, now: Instant) -> bool {
        let state = self.state();
        state.active == 0
            && state
                .idle_since
                .and_then(|idle_since| now.checked_duration_since(idle_since))
                .is_some_and(|idle| idle >= IDLE_TIMEOUT)
    }

    fn checkout(self: &Arc<Self>) -> Handle {
        let mut state = self.state();
        state.active = state
            .active
            .checked_add(1)
            .expect("active HTTP client count overflow");
        state.idle_since = None;
        drop(state);
        Handle {
            client: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut state = self.state();
        debug_assert!(state.active > 0, "released an inactive HTTP client");
        if state.active == 0 {
            return;
        }
        state.active -= 1;
        if state.active == 0 {
            state.idle_since = Some(Instant::now());
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Default)]
struct State {
    active: usize,
    idle_since: Option<Instant>,
}

struct Handle {
    client: Arc<Client>,
}

impl Deref for Handle {
    type Target = reqwest::Client;

    fn deref(&self) -> &Self::Target {
        &self.client.inner
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.client.release();
    }
}

struct Pool {
    clients: HashMap<Key, Arc<Client>>,
    next_cleanup: Instant,
    generation: u64,
}

impl Pool {
    fn new(now: Instant) -> Self {
        Self {
            clients: HashMap::new(),
            next_cleanup: next_cleanup(now),
            generation: 0,
        }
    }

    fn checkout(&mut self, key: &Key, now: Instant) -> Option<Handle> {
        self.cleanup(now);
        self.clients.get(key).map(Client::checkout)
    }

    fn checkout_or_insert(
        &mut self,
        key: Key,
        client: Arc<Client>,
        generation: u64,
        now: Instant,
    ) -> Handle {
        self.cleanup(now);
        if self.generation != generation {
            return client.checkout();
        }
        self.clients.entry(key).or_insert(client).checkout()
    }

    fn cleanup(&mut self, now: Instant) {
        if now < self.next_cleanup {
            return;
        }
        self.clients.retain(|_, client| !client.expired(now));
        self.next_cleanup = next_cleanup(now);
    }

    fn clear(&mut self, now: Instant) {
        self.clients.clear();
        self.next_cleanup = next_cleanup(now);
        self.generation = self.generation.wrapping_add(1);
    }
}

fn next_cleanup(now: Instant) -> Instant {
    now.checked_add(IDLE_TIMEOUT).unwrap_or(now)
}

pub struct Http {
    pool: Mutex<Pool>,
}

impl Http {
    pub fn new() -> Self {
        Self::default()
    }

    fn client(&self, request: &net::Request) -> Result<Handle, downloader::Error> {
        self.client_at(request, Instant::now())
    }

    fn client_at(&self, request: &net::Request, now: Instant) -> Result<Handle, downloader::Error> {
        let key = Key::from(request);
        let generation = {
            let mut pool = self.pool();
            if let Some(client) = pool.checkout(&key, now) {
                return Ok(client);
            }
            pool.generation
        };

        // Build outside the pool lock so unrelated proxies do not block; duplicate cold builds
        // are merged by the second lookup instead of using singleflight.
        let client = Arc::new(Client::new(build_client(&key)?));
        Ok(self.pool().checkout_or_insert(key, client, generation, now))
    }

    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for Http {
    fn default() -> Self {
        Self {
            pool: Mutex::new(Pool::new(Instant::now())),
        }
    }
}

impl downloader::Download for Http {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.pool().clear(Instant::now());
        Ok(())
    }

    async fn fetch(&self, mut request: net::Request) -> Result<net::Response, downloader::Error> {
        let client = self.client(&request)?;
        let mut url = url::Url::parse(&request.url)
            .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
        let mut method = reqwest::Method::from(&request.method);
        let mut body = request.body.clone();
        let mut inherit_headers = true;
        let mut cookies = request.cookies.clone();
        let mut redirects = Vec::new();

        let response = loop {
            let headers = request_headers(&request, inherit_headers, &cookies, &method)?;
            let mut builder = client.request(method.clone(), url.clone()).headers(headers);
            if let Some(timeout) = request.timeout {
                builder = builder.timeout(Duration::from_millis(timeout));
            }
            builder = with_body(builder, &body);

            let response = builder.send().await?;
            merge_response_cookies(&mut cookies, &response);
            let Some(location) = redirect_location(&response)? else {
                break response;
            };
            if redirects.len() >= MAX_REDIRECTS {
                return Err(downloader::Error::TooManyRedirects);
            }
            let target = response
                .url()
                .join(&location)
                .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
            if !request.allows(&target) {
                return Err(downloader::Error::DisallowedRedirect(target.to_string()));
            }
            if response.url().origin() != target.origin() {
                inherit_headers = false;
                cookies.clear();
            }
            redirect_method(response.status(), &mut method, &mut body);
            redirects.push(target.to_string());
            url = target;
        };
        let url = response.url().to_string();
        let status = net::StatusCode(response.status().as_u16());
        let reason = response.status().canonical_reason().map(str::to_string);
        let version = match response.version() {
            reqwest::Version::HTTP_09 | reqwest::Version::HTTP_10 => net::HttpVersion::Http10,
            reqwest::Version::HTTP_11 => net::HttpVersion::Http11,
            reqwest::Version::HTTP_2 => net::HttpVersion::Http2,
            reqwest::Version::HTTP_3 => net::HttpVersion::Http3,
            _ => net::HttpVersion::Http11,
        };
        let headers = response
            .headers()
            .iter()
            .filter_map(|(key, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (key.to_string(), value.to_string()))
            })
            .collect();
        let body = response.bytes().await?;
        if !inherit_headers {
            request.headers.clear();
        }
        request.cookies.clone_from(&cookies);
        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status,
            reason,
            version,
            redirects,
            headers,
            cookies,
            body,
        })
    }
}

fn build_client(key: &Key) -> Result<reqwest::Client, downloader::Error> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(IDLE_TIMEOUT);

    if let Some(proxy) = &key.proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
    }

    builder = builder.danger_accept_invalid_certs(key.accept_invalid_certs);

    Ok(builder.build()?)
}

fn request_headers(
    request: &net::Request,
    inherit_headers: bool,
    cookies: &net::Cookies,
    method: &reqwest::Method,
) -> Result<HeaderMap, downloader::Error> {
    let mut headers = if inherit_headers {
        to_header_map(&request.headers)?
    } else {
        HeaderMap::new()
    };
    if method == reqwest::Method::GET || method == reqwest::Method::HEAD {
        headers.remove(reqwest::header::CONTENT_LENGTH);
        headers.remove(reqwest::header::CONTENT_TYPE);
        headers.remove(reqwest::header::TRANSFER_ENCODING);
    }
    if !cookies.is_empty() {
        let value = cookies
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("; ");
        headers.remove(reqwest::header::COOKIE);
        headers.insert(reqwest::header::COOKIE, HeaderValue::from_str(&value)?);
    }
    Ok(headers)
}

fn merge_response_cookies(cookies: &mut net::Cookies, response: &reqwest::Response) {
    for cookie in response.cookies() {
        if cookie.value().is_empty() {
            cookies.remove(cookie.name());
        } else {
            cookies.insert(cookie.name().to_string(), cookie.value().to_string());
        }
    }
}

fn with_body(builder: reqwest::RequestBuilder, body: &net::Body) -> reqwest::RequestBuilder {
    match body {
        net::Body::Empty => builder,
        net::Body::Bytes(bytes) => builder.body(bytes.clone()),
        net::Body::Text(text) => builder.body(text.clone()),
        net::Body::Json(value) => builder.json(value),
    }
}

fn redirect_location(response: &reqwest::Response) -> Result<Option<String>, downloader::Error> {
    if !response.status().is_redirection() {
        return Ok(None);
    }
    let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
        return Ok(None);
    };
    location
        .to_str()
        .map(|location| Some(location.to_string()))
        .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))
}

fn redirect_method(
    status: reqwest::StatusCode,
    method: &mut reqwest::Method,
    body: &mut net::Body,
) {
    let switch_to_get = status == reqwest::StatusCode::SEE_OTHER
        || ((status == reqwest::StatusCode::MOVED_PERMANENTLY
            || status == reqwest::StatusCode::FOUND)
            && *method == reqwest::Method::POST);
    if switch_to_get && *method != reqwest::Method::HEAD {
        *method = reqwest::Method::GET;
        *body = net::Body::Empty;
    }
}

fn to_header_map(headers: &net::Headers) -> Result<HeaderMap, downloader::Error> {
    let mut map = HeaderMap::new();

    for (key, value) in headers {
        map.insert(
            HeaderName::from_bytes(key.as_bytes())?,
            HeaderValue::from_str(value)?,
        );
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Barrier, mpsc};
    use std::thread;

    use super::*;
    use crate::downloader::Download;

    fn listener() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];

        loop {
            let read = stream.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        String::from_utf8(bytes).unwrap()
    }

    fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn authenticated_proxy(proxy: &str, username: &str, password: &str) -> String {
        let mut proxy = url::Url::parse(proxy).unwrap();
        proxy.set_username(username).unwrap();
        proxy.set_password(Some(password)).unwrap();
        proxy.to_string()
    }

    fn through_proxy(target: &str, proxy: &str) -> net::Request {
        let mut request = net::Request::follow(target).unwrap();
        request.proxy = Some(net::ProxyConfig {
            url: proxy.to_string(),
        });
        request
    }

    #[tokio::test]
    async fn sends_structured_cookies_and_collects_response_cookies() {
        let (listener, base_url) = listener();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            request_tx.send(read_request(&mut stream)).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nSet-Cookie: token=abc; Path=/\r\nConnection: close\r\n\r\nok",
                )
                .unwrap();
        });

        let mut request = net::Request::follow(base_url).unwrap();
        request
            .headers
            .insert("cookie".to_string(), "manual=old".to_string());
        request.cookies.insert("sid".to_string(), "1".to_string());

        let response = Http::new().fetch(request).await.unwrap();
        let raw_request = request_rx.recv().unwrap().to_ascii_lowercase();
        server.join().unwrap();

        assert!(raw_request.contains("cookie: sid=1\r\n"));
        assert!(!raw_request.contains("manual=old"));
        assert_eq!(
            response.cookies.get("token").map(String::as_str),
            Some("abc")
        );
        assert_eq!(response.body.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn follows_redirects_and_records_the_redirect_targets() {
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let first_request = read_request(&mut first);
            assert!(first_request.starts_with("GET /start "));
            first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();

            let (mut second, _) = listener.accept().unwrap();
            let second_request = read_request(&mut second);
            assert!(second_request.starts_with("GET /final "));
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone")
                .unwrap();
        });

        let response = Http::new()
            .fetch(net::Request::follow(format!("{base_url}/start")).unwrap())
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(response.url, format!("{base_url}/final"));
        assert_eq!(response.redirects, [format!("{base_url}/final")]);
    }

    #[tokio::test]
    async fn same_origin_redirect_carries_response_cookies_to_the_next_hop() {
        let (listener, base_url) = listener();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let _ = read_request(&mut first);
            first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /final\r\nSet-Cookie: session=abc; Path=/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();

            let (mut second, _) = listener.accept().unwrap();
            request_tx.send(read_request(&mut second)).unwrap();
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });

        let response = Http::new()
            .fetch(net::Request::follow(format!("{base_url}/start")).unwrap())
            .await
            .unwrap();
        server.join().unwrap();
        let redirected = request_rx.recv().unwrap().to_ascii_lowercase();

        assert!(redirected.contains("cookie: session=abc\r\n"));
        assert_eq!(
            response.cookies.get("session").map(String::as_str),
            Some("abc")
        );
        assert_eq!(
            response.request.cookies.get("session").map(String::as_str),
            Some("abc")
        );
    }

    #[tokio::test]
    async fn rejects_disallowed_redirect_before_sending_it() {
        let (source, source_url) = listener();
        let (target, target_url) = listener();
        target.set_nonblocking(true).unwrap();
        let target_url = target_url.replacen("127.0.0.1", "localhost", 1);
        let server = thread::spawn(move || {
            let (mut stream, _) = source.accept().unwrap();
            let _ = read_request(&mut stream);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        let mut request = net::Request::follow(source_url).unwrap();
        request.set_allowed_domains(vec!["127.0.0.1".to_string()]);

        let error = Http::new().fetch(request).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, downloader::Error::DisallowedRedirect(_)));
        thread::sleep(Duration::from_millis(20));
        assert!(matches!(
            target.accept(),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[tokio::test]
    async fn cross_origin_redirect_sends_no_inherited_credentials() {
        let (source, source_url) = listener();
        let (target, target_url) = listener();
        let target_url = target_url.replacen("127.0.0.1", "localhost", 1);
        let (request_tx, request_rx) = mpsc::channel();
        let source_server = thread::spawn(move || {
            let (mut stream, _) = source.accept().unwrap();
            let _ = read_request(&mut stream);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        let target_server = thread::spawn(move || {
            let (mut stream, _) = target.accept().unwrap();
            request_tx.send(read_request(&mut stream)).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let mut request = net::Request::follow(source_url).unwrap();
        request
            .headers
            .insert("x-secret".to_string(), "hidden".to_string());
        request.cookies.insert("sid".to_string(), "1".to_string());
        request.set_allowed_domains(vec!["127.0.0.1".to_string(), "localhost".to_string()]);

        Http::new().fetch(request).await.unwrap();
        source_server.join().unwrap();
        target_server.join().unwrap();
        let redirected = request_rx.recv().unwrap().to_ascii_lowercase();

        assert!(!redirected.contains("x-secret:"));
        assert!(!redirected.contains("cookie:"));
    }

    #[tokio::test]
    async fn applies_request_timeout() {
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            thread::sleep(Duration::from_millis(100));
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
        });
        let mut request = net::Request::follow(base_url).unwrap();
        request.timeout = Some(10);

        let error = Http::new().fetch(request).await.unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, downloader::Error::Http(error) if error.is_timeout()));
    }

    #[tokio::test]
    async fn returns_truncated_response_body_errors() {
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc")
                .unwrap();
        });

        let error = Http::new()
            .fetch(net::Request::follow(base_url).unwrap())
            .await
            .unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, downloader::Error::Http(_)));
    }

    #[tokio::test]
    async fn routes_http_requests_through_the_configured_proxy() {
        let (proxy, proxy_url) = listener();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = proxy.accept().unwrap();
            request_tx.send(read_request(&mut stream)).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nproxied",
                )
                .unwrap();
        });

        let target = "http://origin.invalid/article?id=7";
        let response = Http::new()
            .fetch(through_proxy(target, &proxy_url))
            .await
            .unwrap();
        server.join().unwrap();
        let request = request_rx.recv().unwrap();

        assert!(request.starts_with("GET http://origin.invalid/article?id=7 HTTP/1.1\r\n"));
        assert_eq!(response.url, target);
        assert_eq!(response.body.as_ref(), b"proxied");
    }

    #[tokio::test]
    async fn proxy_url_credentials_are_sent_and_isolated() {
        let (proxy, proxy_url) = listener();
        let user_proxy = authenticated_proxy(&proxy_url, "user", "pass");
        let admin_proxy = authenticated_proxy(&proxy_url, "admin", "admin");
        let user = through_proxy("http://one.invalid/", &user_proxy);
        let admin = through_proxy("http://two.invalid/", &admin_proxy);
        let http = Http::new();

        let user_client = http.client(&user).unwrap();
        let admin_client = http.client(&admin).unwrap();
        assert!(!Arc::ptr_eq(&user_client.client, &admin_client.client));
        drop(user_client);
        drop(admin_client);

        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = proxy.accept().unwrap();
                request_tx.send(read_request(&mut stream)).unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .unwrap();
            }
        });

        http.fetch(user).await.unwrap();
        http.fetch(admin).await.unwrap();
        server.join().unwrap();
        let user_request = request_rx.recv().unwrap();
        let admin_request = request_rx.recv().unwrap();

        assert_eq!(
            header(&user_request, "proxy-authorization"),
            Some("Basic dXNlcjpwYXNz")
        );
        assert_eq!(
            header(&admin_request, "proxy-authorization"),
            Some("Basic YWRtaW46YWRtaW4=")
        );
    }

    #[tokio::test]
    async fn every_redirect_hop_uses_the_configured_proxy() {
        let (proxy, proxy_url) = listener();
        let proxy_url = authenticated_proxy(&proxy_url, "user", "pass");
        let target = "http://first.invalid/start";
        let redirected = "http://second.invalid/final";
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = proxy.accept().unwrap();
            request_tx.send(read_request(&mut first)).unwrap();
            first
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {redirected}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();

            let (mut second, _) = proxy.accept().unwrap();
            request_tx.send(read_request(&mut second)).unwrap();
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndone")
                .unwrap();
        });
        let mut request = through_proxy(target, &proxy_url);
        request
            .headers
            .insert("x-secret".to_string(), "hidden".to_string());
        request.cookies.insert("sid".to_string(), "1".to_string());
        request.set_allowed_domains(vec![
            "first.invalid".to_string(),
            "second.invalid".to_string(),
        ]);

        let response = Http::new().fetch(request).await.unwrap();
        server.join().unwrap();
        let first = request_rx.recv().unwrap();
        let second = request_rx.recv().unwrap();

        assert!(first.starts_with("GET http://first.invalid/start HTTP/1.1\r\n"));
        assert!(second.starts_with("GET http://second.invalid/final HTTP/1.1\r\n"));
        assert_eq!(
            header(&first, "proxy-authorization"),
            Some("Basic dXNlcjpwYXNz")
        );
        assert_eq!(
            header(&second, "proxy-authorization"),
            Some("Basic dXNlcjpwYXNz")
        );
        assert_eq!(header(&first, "x-secret"), Some("hidden"));
        assert_eq!(header(&first, "cookie"), Some("sid=1"));
        assert_eq!(header(&second, "x-secret"), None);
        assert_eq!(header(&second, "cookie"), None);
        assert_eq!(response.url, redirected);
        assert_eq!(response.redirects, [redirected]);
    }

    fn proxied(url: &str) -> net::Request {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.proxy = Some(net::ProxyConfig {
            url: url.to_string(),
        });
        request
    }

    #[test]
    fn reuses_the_same_key_and_isolates_different_proxies() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let other = proxied("http://127.0.0.1:8081");

        let first = http.client(&request).unwrap();
        let second = http.client(&request).unwrap();
        let other = http.client(&other).unwrap();

        assert!(Arc::ptr_eq(&first.client, &second.client));
        assert!(!Arc::ptr_eq(&first.client, &other.client));
        assert_eq!(http.pool().clients.len(), 2);
    }

    #[test]
    fn active_client_survives_cleanup_after_the_idle_timeout() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let now = Instant::now();

        let first = http.client_at(&request, now).unwrap();
        let second = http.client_at(&request, now + IDLE_TIMEOUT).unwrap();

        assert!(Arc::ptr_eq(&first.client, &second.client));
        assert_eq!(http.pool().clients.len(), 1);
        assert_eq!(first.client.state().active, 2);
    }

    #[test]
    fn released_client_is_replaced_after_it_expires() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let trigger = proxied("http://127.0.0.1:8081");
        let now = Instant::now();
        let handle = http.client_at(&request, now).unwrap();
        let expired = Arc::clone(&handle.client);

        drop(handle);
        assert_eq!(expired.state().active, 0);
        let idle_since = expired.state().idle_since.unwrap();

        let cleanup = idle_since + IDLE_TIMEOUT;
        let _trigger = http.client_at(&trigger, cleanup).unwrap();
        assert!(!http.pool().clients.contains_key(&Key::from(&request)));

        let replacement = http.client_at(&request, cleanup).unwrap();
        assert!(!Arc::ptr_eq(&expired, &replacement.client));
    }

    #[tokio::test]
    async fn close_clears_the_pool_without_invalidating_checked_out_clients() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let client = http.client(&request).unwrap();
        assert_eq!(http.pool().clients.len(), 1);

        http.close().await.unwrap();

        assert!(http.pool().clients.is_empty());
        assert!(client.get("https://example.com").build().is_ok());

        let replacement = http.client(&request).unwrap();
        assert!(!Arc::ptr_eq(&client.client, &replacement.client));
        drop(client);
        assert_eq!(replacement.client.state().active, 1);
    }

    #[tokio::test]
    async fn stale_build_after_close_is_not_inserted_into_the_new_generation() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let key = Key::from(&request);
        let now = Instant::now();
        let generation = {
            let mut pool = http.pool();
            assert!(pool.checkout(&key, now).is_none());
            pool.generation
        };
        let stale = Arc::new(Client::new(build_client(&key).unwrap()));

        http.close().await.unwrap();
        let handle =
            http.pool()
                .checkout_or_insert(key.clone(), Arc::clone(&stale), generation, now);

        assert!(Arc::ptr_eq(&handle.client, &stale));
        assert!(http.pool().clients.is_empty());
        assert!(handle.get("https://example.com").build().is_ok());

        let current = http.client(&request).unwrap();
        assert!(!Arc::ptr_eq(&handle.client, &current.client));
        drop(handle);
        assert_eq!(current.client.state().active, 1);
        assert!(Arc::ptr_eq(
            http.pool().clients.get(&key).unwrap(),
            &current.client
        ));
    }

    #[test]
    fn proxy_and_tls_are_both_part_of_the_key() {
        let http = Http::new();
        let request = proxied("http://127.0.0.1:8080");
        let other_proxy = proxied("http://127.0.0.1:8081");
        let mut insecure = request.clone();
        insecure.tls = Some(net::TlsConfig {
            accept_invalid_certs: true,
        });

        let client = http.client(&request).unwrap();
        let proxy = http.client(&other_proxy).unwrap();
        let tls = http.client(&insecure).unwrap();

        assert!(!Arc::ptr_eq(&client.client, &proxy.client));
        assert!(!Arc::ptr_eq(&client.client, &tls.client));
        assert!(!Arc::ptr_eq(&proxy.client, &tls.client));
        assert_eq!(http.pool().clients.len(), 3);
    }

    #[test]
    fn concurrent_checkout_of_the_same_key_uses_one_client() {
        const WORKERS: usize = 16;

        let http = Arc::new(Http::new());
        let barrier = Arc::new(Barrier::new(WORKERS));
        let threads = (0..WORKERS)
            .map(|_| {
                let http = Arc::clone(&http);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let request = proxied("http://127.0.0.1:8080");
                    barrier.wait();
                    http.client(&request).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let clients = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(http.pool().clients.len(), 1);
        assert!(
            clients
                .iter()
                .skip(1)
                .all(|client| Arc::ptr_eq(&clients[0].client, &client.client))
        );
    }
}
