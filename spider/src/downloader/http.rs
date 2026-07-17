use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::{downloader, net};

const MAX_REDIRECTS: usize = 10;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ClientKey {
    proxy: Option<String>,
    accept_invalid_certs: bool,
}

#[derive(Default)]
pub struct Http {
    clients: Mutex<HashMap<ClientKey, reqwest::Client>>,
}

impl Http {
    pub fn new() -> Self {
        Self::default()
    }

    fn client(&self, request: &net::Request) -> Result<reqwest::Client, downloader::Error> {
        let key = ClientKey {
            proxy: request.proxy.as_ref().map(|proxy| proxy.url.clone()),
            accept_invalid_certs: request
                .tls
                .as_ref()
                .is_some_and(|tls| tls.accept_invalid_certs),
        };

        if let Some(client) = self.clients().get(&key) {
            return Ok(client.clone());
        }

        let client = build_client(&key)?;
        Ok(self.clients().entry(key).or_insert(client).clone())
    }

    fn clients(&self) -> MutexGuard<'_, HashMap<ClientKey, reqwest::Client>> {
        self.clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl downloader::Download for Http {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
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

fn build_client(key: &ClientKey) -> Result<reqwest::Client, downloader::Error> {
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());

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
    use std::sync::mpsc;
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

    #[test]
    fn reuses_clients_with_the_same_transport_configuration() {
        let http = Http::new();
        let request = net::Request::follow("https://example.com").unwrap();

        let first = http.client(&request).unwrap();
        let second = http.client(&request).unwrap();

        assert_eq!(http.clients().len(), 1);
        drop(first);
        drop(second);

        let mut proxied = request;
        proxied.proxy = Some(net::ProxyConfig {
            url: "http://127.0.0.1:8080".to_string(),
        });
        http.client(&proxied).unwrap();

        assert_eq!(http.clients().len(), 2);
    }
}
