use http::header::HeaderMap;
use std::time::Duration;

use crate::{downloader, net};

mod body;
mod pool;

use pool::Clients;

const MAX_REDIRECTS: usize = 10;
const MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;

pub struct Http {
    clients: Clients,
    max_body_bytes: u64,
}

impl Default for Http {
    fn default() -> Self {
        Self {
            clients: Clients::default(),
            max_body_bytes: MAX_BODY_BYTES,
        }
    }
}

impl Http {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_idle_clients(self, max_idle_clients: usize) -> Result<Self, downloader::Error> {
        self.clients.set_max_idle_clients(max_idle_clients)?;
        Ok(self)
    }

    pub fn with_max_body_bytes(mut self, max_body_bytes: u64) -> Result<Self, downloader::Error> {
        if max_body_bytes == 0 {
            return Err(downloader::Error::InvalidConfig(
                "max_body_bytes must be positive".to_string(),
            ));
        }
        self.max_body_bytes = max_body_bytes;
        Ok(self)
    }

    fn body_limit(&self, request: &net::Request) -> Result<u64, downloader::Error> {
        match request.max_body_bytes {
            Some(0) => Err(downloader::Error::InvalidConfig(
                "Request max_body_bytes must be positive".to_string(),
            )),
            Some(limit) if limit > self.max_body_bytes => {
                Err(downloader::Error::InvalidConfig(format!(
                    "Request max_body_bytes ({limit}) exceeds the Worker limit ({})",
                    self.max_body_bytes
                )))
            }
            Some(limit) => Ok(limit),
            None => Ok(self.max_body_bytes),
        }
    }

    async fn download(
        &self,
        mut request: net::Request,
        body_limit: u64,
    ) -> Result<net::Response, downloader::Error> {
        let client = self.clients.get(&request)?;
        let mut url = url::Url::parse(&request.url)
            .map_err(|error| downloader::Error::InvalidRedirect(error.to_string()))?;
        let mut method = reqwest::Method::from(&request.method);
        let mut request_body = request.body.clone();
        let mut inherit_headers = true;
        let mut strip_body_headers = false;
        let mut cookies = request.cookies.clone();
        let mut redirects = Vec::new();

        let response = loop {
            let headers = request_headers(
                &request,
                inherit_headers,
                strip_body_headers,
                &cookies,
                &url,
            )?;
            let builder = client.request(method.clone(), url.clone()).headers(headers);
            let response = with_body(builder, &request_body).send().await?;
            let response_headers = net::Headers::from(response.headers().clone());
            cookies.store_response(response.url(), &response_headers);
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
            validate_redirect(&target)?;
            if !request.allows(&target) {
                return Err(downloader::Error::DisallowedRedirect(
                    target.origin().ascii_serialization(),
                ));
            }
            if response.url().origin() != target.origin() {
                inherit_headers = false;
                cookies = cookies.for_url(&target);
            }
            strip_body_headers |=
                redirect_method(response.status(), &mut method, &mut request_body);
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
        let headers = net::Headers::from(response.headers().clone());
        let body = body::read(response, body_limit).await?;
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

impl downloader::Download for Http {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.clients.clear();
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let body_limit = self.body_limit(&request)?;
        let Some(timeout) = request.timeout else {
            return self.download(request, body_limit).await;
        };
        let deadline = tokio::time::Instant::now()
            .checked_add(Duration::from_millis(timeout))
            .ok_or_else(|| {
                downloader::Error::InvalidConfig(
                    "Request timeout exceeds the supported duration".to_string(),
                )
            })?;
        tokio::time::timeout_at(deadline, self.download(request, body_limit))
            .await
            .map_err(|_| downloader::Error::Timeout)?
    }
}

fn request_headers(
    request: &net::Request,
    inherit_headers: bool,
    strip_body_headers: bool,
    cookies: &net::Cookies,
    url: &url::Url,
) -> Result<HeaderMap, downloader::Error> {
    let mut headers = if inherit_headers {
        request.headers.clone().into_map()
    } else {
        HeaderMap::new()
    };
    if strip_body_headers {
        headers.remove(reqwest::header::CONTENT_LENGTH);
        headers.remove(reqwest::header::CONTENT_TYPE);
        headers.remove(reqwest::header::CONTENT_ENCODING);
        headers.remove(reqwest::header::CONTENT_LANGUAGE);
        headers.remove(reqwest::header::CONTENT_LOCATION);
        headers.remove(reqwest::header::TRANSFER_ENCODING);
    }
    headers.remove(reqwest::header::COOKIE);
    if let Some(value) = cookies.request_header(url)? {
        headers.insert(reqwest::header::COOKIE, value);
    }
    Ok(headers)
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
    if !matches!(
        response.status(),
        reqwest::StatusCode::MOVED_PERMANENTLY
            | reqwest::StatusCode::FOUND
            | reqwest::StatusCode::SEE_OTHER
            | reqwest::StatusCode::TEMPORARY_REDIRECT
            | reqwest::StatusCode::PERMANENT_REDIRECT
    ) {
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
) -> bool {
    let switch_to_get = status == reqwest::StatusCode::SEE_OTHER
        || ((status == reqwest::StatusCode::MOVED_PERMANENTLY
            || status == reqwest::StatusCode::FOUND)
            && *method == reqwest::Method::POST);
    if switch_to_get && *method != reqwest::Method::HEAD {
        *method = reqwest::Method::GET;
        *body = net::Body::Empty;
        return true;
    }
    false
}

fn validate_redirect(target: &url::Url) -> Result<(), downloader::Error> {
    if !matches!(target.scheme(), "http" | "https") {
        return Err(downloader::Error::InvalidRedirect(format!(
            "unsupported protocol: {}",
            target.scheme()
        )));
    }
    if !target.has_host() {
        return Err(downloader::Error::InvalidRedirect(
            "redirect URL must have a host".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, mpsc};
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

    #[test]
    fn request_headers_preserve_repeated_values() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.headers.try_append("x-value", "one").unwrap();
        request.headers.try_append("X-Value", "two").unwrap();
        let url = url::Url::parse(&request.url).unwrap();

        let headers = request_headers(&request, true, false, &request.cookies, &url).unwrap();

        assert_eq!(
            headers
                .get_all("x-value")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );
    }

    #[test]
    fn request_headers_accept_cookies_only_from_the_cookie_store() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.headers.try_set("cookie", "raw=stale").unwrap();
        let url = url::Url::parse(&request.url).unwrap();

        let headers = request_headers(&request, true, false, &request.cookies, &url).unwrap();

        assert!(!headers.contains_key(reqwest::header::COOKIE));
    }

    #[test]
    fn request_headers_strip_entity_headers_only_after_a_body_is_discarded() {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.headers.try_set("content-length", "4").unwrap();
        request
            .headers
            .try_set("content-type", "text/plain")
            .unwrap();
        request
            .headers
            .try_set("content-encoding", "identity")
            .unwrap();
        request.headers.try_set("content-language", "en").unwrap();
        request
            .headers
            .try_set("content-location", "/source")
            .unwrap();
        request
            .headers
            .try_set("transfer-encoding", "chunked")
            .unwrap();
        let url = url::Url::parse(&request.url).unwrap();

        let preserved = request_headers(&request, true, false, &request.cookies, &url).unwrap();
        let stripped = request_headers(&request, true, true, &request.cookies, &url).unwrap();

        for name in [
            reqwest::header::CONTENT_LENGTH,
            reqwest::header::CONTENT_TYPE,
            reqwest::header::CONTENT_ENCODING,
            reqwest::header::CONTENT_LANGUAGE,
            reqwest::header::CONTENT_LOCATION,
            reqwest::header::TRANSFER_ENCODING,
        ] {
            assert!(preserved.contains_key(&name));
            assert!(!stripped.contains_key(&name));
        }
    }

    #[test]
    fn redirect_to_get_strips_body_headers_even_when_the_body_is_empty() {
        let mut method = reqwest::Method::POST;
        let mut body = net::Body::Empty;

        assert!(redirect_method(
            reqwest::StatusCode::FOUND,
            &mut method,
            &mut body
        ));
        assert_eq!(method, reqwest::Method::GET);
        assert!(matches!(body, net::Body::Empty));
    }

    #[test]
    fn default_body_limit_is_64_mib() {
        assert_eq!(Http::new().max_body_bytes, 64 * 1024 * 1024);
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

        let request_url = url::Url::parse(&base_url).unwrap();
        let mut request = net::Request::follow(base_url).unwrap();
        request.headers.try_set("cookie", "manual=old").unwrap();
        request.cookies.insert(&request_url, "sid", "1").unwrap();

        let response = Http::new().fetch(request).await.unwrap();
        let raw_request = request_rx.recv().unwrap().to_ascii_lowercase();
        server.join().unwrap();

        assert!(raw_request.contains("cookie: sid=1\r\n"));
        assert!(!raw_request.contains("manual=old"));
        let response_url = url::Url::parse(&response.url).unwrap();
        assert_eq!(response.cookies.get(&response_url, "token"), Some("abc"));
        assert_eq!(response.body.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn preserves_encoded_body_and_exposes_decoded_text() {
        const BODY: &[u8] = b"<html><h1>\xB9\xF0\xC1\xD6\xC3\xD7\xB7\xDB</h1></html>";
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=gbk\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                BODY.len()
            );
            stream.write_all(headers.as_bytes()).unwrap();
            stream.write_all(BODY).unwrap();
        });

        let response = Http::new()
            .fetch(net::Request::follow(base_url).unwrap())
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(response.body().as_ref(), BODY);
        assert_eq!(
            response.css().unwrap().find("h1").unwrap().unwrap().text(),
            "桂林米粉"
        );
        assert_eq!(
            response
                .headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/html; charset=gbk")
        );
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
    async fn does_not_follow_a_304_location() {
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            assert!(request.starts_with("GET /cached "));
            stream
                .write_all(
                    b"HTTP/1.1 304 Not Modified\r\nLocation: /other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });

        let response = Http::new()
            .fetch(net::Request::follow(format!("{base_url}/cached")).unwrap())
            .await
            .unwrap();
        server.join().unwrap();

        assert_eq!(response.status, net::StatusCode(304));
        assert_eq!(response.url, format!("{base_url}/cached"));
        assert!(response.redirects.is_empty());
    }

    #[tokio::test]
    async fn initial_get_with_a_body_preserves_content_type() {
        let (listener, base_url) = listener();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            request_tx.send(read_request(&mut stream)).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let request = net::Request::follow(base_url)
            .unwrap()
            .header("content-type", "text/plain; charset=utf-8")
            .unwrap()
            .body(net::Body::Text("query".to_string()));

        Http::new().fetch(request).await.unwrap();
        server.join().unwrap();
        let request = request_rx.recv().unwrap();

        assert!(request.starts_with("GET / HTTP/1.1\r\n"));
        assert_eq!(
            header(&request, "content-type"),
            Some("text/plain; charset=utf-8")
        );
    }

    #[tokio::test]
    async fn redirect_to_get_removes_headers_for_the_discarded_body() {
        let (listener, base_url) = listener();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            request_tx.send(read_request(&mut first)).unwrap();
            first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();

            let (mut second, _) = listener.accept().unwrap();
            request_tx.send(read_request(&mut second)).unwrap();
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let request = net::Request::follow(format!("{base_url}/start"))
            .unwrap()
            .method(net::Method::Post)
            .header("content-type", "text/plain")
            .unwrap()
            .header("content-encoding", "identity")
            .unwrap()
            .body(net::Body::Text("body".to_string()));

        Http::new().fetch(request).await.unwrap();
        server.join().unwrap();
        let first = request_rx.recv().unwrap();
        let second = request_rx.recv().unwrap();

        assert!(first.starts_with("POST /start HTTP/1.1\r\n"));
        assert_eq!(header(&first, "content-type"), Some("text/plain"));
        assert!(second.starts_with("GET /final HTTP/1.1\r\n"));
        for name in [
            "content-length",
            "content-type",
            "content-encoding",
            "content-language",
            "content-location",
            "transfer-encoding",
        ] {
            assert_eq!(header(&second, name), None, "unexpected {name}");
        }
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
        let response_url = url::Url::parse(&response.url).unwrap();
        assert_eq!(response.cookies.get(&response_url, "session"), Some("abc"));
        assert_eq!(
            response.request.cookies.get(&response_url, "session"),
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
    async fn rejects_a_redirect_to_an_unsupported_protocol() {
        let (listener, base_url) = listener();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: file:///private/data\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });

        let error = Http::new()
            .fetch(net::Request::follow(base_url).unwrap())
            .await
            .unwrap_err();
        server.join().unwrap();

        assert!(matches!(
            error,
            downloader::Error::InvalidRedirect(message)
                if message == "unsupported protocol: file"
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
        let request_url = url::Url::parse(&source_url).unwrap();
        let mut request = net::Request::follow(source_url).unwrap();
        request.headers.try_set("x-secret", "hidden").unwrap();
        request.cookies.insert(&request_url, "sid", "1").unwrap();
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

        assert!(matches!(error, downloader::Error::Timeout));
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

        let user_client = http.clients.get(&user).unwrap();
        let admin_client = http.clients.get(&admin).unwrap();
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
        request.headers.try_set("x-secret", "hidden").unwrap();
        request
            .cookies
            .insert(&url::Url::parse(target).unwrap(), "sid", "1")
            .unwrap();
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

    #[tokio::test]
    async fn cross_origin_redirect_keeps_an_applicable_domain_cookie() {
        let (proxy, proxy_url) = listener();
        let source = "http://www.example.test/start";
        let target = "http://api.example.test/final";
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = proxy.accept().unwrap();
            request_tx.send(read_request(&mut first)).unwrap();
            first
                .write_all(
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: {target}\r\nSet-Cookie: shared=one; Domain=example.test; Path=/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .unwrap();

            let (mut second, _) = proxy.accept().unwrap();
            request_tx.send(read_request(&mut second)).unwrap();
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        });
        let mut request = through_proxy(source, &proxy_url);
        request.headers.try_set("x-secret", "hidden").unwrap();
        request.set_allowed_domains(vec![
            "www.example.test".to_string(),
            "api.example.test".to_string(),
        ]);

        let response = Http::new().fetch(request).await.unwrap();
        server.join().unwrap();
        let first = request_rx.recv().unwrap();
        let second = request_rx.recv().unwrap();

        assert_eq!(header(&first, "x-secret"), Some("hidden"));
        assert_eq!(header(&first, "cookie"), None);
        assert_eq!(header(&second, "x-secret"), None);
        assert_eq!(header(&second, "cookie"), Some("shared=one"));
        assert_eq!(
            response
                .cookies
                .get(&url::Url::parse(target).unwrap(), "shared"),
            Some("one")
        );
    }
}
