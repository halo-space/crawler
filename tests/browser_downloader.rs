use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use macros::spider;
use spider::downloader::Download;
use spider::downloader::Downloader;
use spider::downloader::browser::Browser;
use spider::net::{Mode, Request, Response};
use spider::{Error, engine};

fn local_page() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        let body = r#"<!doctype html>
<html><body><div id="app">loading</div>
<script>setTimeout(() => document.querySelector('#app').textContent = 'rendered', 25)</script>
</body></html>"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
    });
    (format!("http://{address}/"), handle)
}

fn hybrid_pages() -> (String, String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 4096];
            let size = stream.read(&mut request).unwrap_or_default();
            let request = String::from_utf8_lossy(&request[..size]);
            let (content_type, body) = if request.starts_with("GET /browser ") {
                (
                    "text/html",
                    r#"<!doctype html>
<html><body><div id="app">loading</div>
<script>document.querySelector('#app').textContent = 'browser-rendered'</script>
</body></html>"#,
                )
            } else {
                ("text/plain", "http-response")
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (
        format!("http://{address}/http"),
        format!("http://{address}/browser"),
        handle,
    )
}

fn cookie_page() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let mut cookie_requests = 0;
        while cookie_requests < 2 {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 4096];
            let size = stream.read(&mut request).unwrap_or_default();
            let request = String::from_utf8_lossy(&request[..size]);
            if !request.starts_with("GET /cookie ") {
                let response =
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(response.as_bytes());
                continue;
            }
            let index = cookie_requests;
            cookie_requests += 1;
            let cookie_reused = request
                .lines()
                .any(|line| line.eq_ignore_ascii_case("Cookie: session=one"));
            let (headers, body) = if index == 0 {
                ("Set-Cookie: session=one; Path=/\r\n", "cookie-set")
            } else if cookie_reused {
                ("", "cookie-reused")
            } else {
                ("", "cookie-missing")
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{address}/cookie"), handle)
}

fn cross_origin_redirect_pages() -> (
    String,
    Arc<Mutex<String>>,
    thread::JoinHandle<()>,
    thread::JoinHandle<()>,
) {
    let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_address = target_listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(String::new()));
    let captured_target = captured.clone();
    let target = thread::spawn(move || {
        let Ok((mut stream, _)) = target_listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        let size = stream.read(&mut request).unwrap_or_default();
        *captured_target.lock().unwrap() = String::from_utf8_lossy(&request[..size]).into_owned();
        let body = "redirect-target";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let source_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let source_address = source_listener.local_addr().unwrap();
    let target_url = format!("http://{target_address}/target");
    let source = thread::spawn(move || {
        let Ok((mut stream, _)) = source_listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(response.as_bytes());
    });
    (
        format!("http://{source_address}/source"),
        captured,
        source,
        target,
    )
}

#[spider]
struct HybridSpider {
    http_url: String,
    browser_url: String,
    responses: Arc<Mutex<Vec<(Mode, u16, String)>>>,
}

#[spider]
impl HybridSpider {
    fn name(&self) -> &str {
        "browser-hybrid"
    }

    async fn start(&self) -> Result<(), Error> {
        let http = Request::follow(self.http_url.clone())
            .map_err(Error::Net)?
            .mode(Mode::Http)
            .node(Self::index);
        let browser = Request::follow(self.browser_url.clone())
            .map_err(Error::Net)?
            .mode(Mode::Browser)
            .wait_selector("#app")
            .node(Self::index);
        self.tx.request(vec![http, browser]).await
    }

    async fn index(&self, response: Response) -> Result<(), Error> {
        self.responses.lock().unwrap().push((
            response.request.mode.clone(),
            response.status.0,
            String::from_utf8_lossy(response.body()).into_owned(),
        ));
        Ok(())
    }
}

#[tokio::test]
async fn browser_returns_rendered_dom_when_enabled() {
    if std::env::var_os("BROWSER_TESTS").is_none() {
        return;
    }

    let (url, server) = local_page();
    let mut browser = Browser::new()
        .with_idle_timeout(Duration::from_secs(30))
        .unwrap();
    if let Ok(url) = std::env::var("WEBDRIVER_URL") {
        browser = browser.with_webdriver_url(url);
    }
    browser.open().await.unwrap();

    let request = Request::follow(url.clone())
        .unwrap()
        .mode(Mode::Browser)
        .wait_selector("#app");
    let response = browser.fetch(request).await.unwrap();

    assert_eq!(response.status.0, 200);
    assert!(
        response
            .body()
            .windows(b"rendered".len())
            .any(|window| window == b"rendered")
    );

    browser.close().await.unwrap();
    let error = browser
        .fetch(Request::follow(url).unwrap().mode(Mode::Browser))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        spider::downloader::Error::InvalidConfig(message) if message.contains("not open")
    ));
    server.join().unwrap();
}

#[tokio::test]
async fn engine_routes_http_and_browser_requests_together() {
    if std::env::var_os("BROWSER_TESTS").is_none() {
        return;
    }

    let (http_url, browser_url, server) = hybrid_pages();
    let responses = Arc::new(Mutex::new(Vec::new()));
    let browser = Browser::new()
        .with_idle_timeout(Duration::from_secs(30))
        .unwrap();
    let downloader = Downloader::new().with_browser(browser);
    let scheduler = spider::Memory::new().with_modes([Mode::Http, Mode::Browser]);
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(HybridSpider::new(http_url, browser_url, responses.clone()))
        .build();

    engine.start_until_idle().await.unwrap();
    server.join().unwrap();

    let mut responses = responses.lock().unwrap().clone();
    responses.sort_by_key(|(mode, _, _)| matches!(mode, Mode::Browser));
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].0, Mode::Http);
    assert_eq!(responses[0].1, 200);
    assert!(responses[0].2.contains("http-response"));
    assert_eq!(responses[1].0, Mode::Browser);
    assert_eq!(responses[1].1, 200);
    assert!(responses[1].2.contains("browser-rendered"));
}

#[tokio::test]
async fn browser_reuses_session_cookies_between_requests() {
    if std::env::var_os("BROWSER_TESTS").is_none() {
        return;
    }

    let (url, server) = cookie_page();
    let browser = Browser::new();
    browser.open().await.unwrap();

    let mut first = Request::follow(url.clone()).unwrap().mode(Mode::Browser);
    first.task_id = "cookie-task".to_string();
    first.trace_id = "cookie-trace".to_string();
    let first_response = browser.fetch(first).await.unwrap();
    assert!(String::from_utf8_lossy(first_response.body()).contains("cookie-set"));

    let mut second = Request::follow(url).unwrap().mode(Mode::Browser);
    second.task_id = "cookie-task".to_string();
    second.trace_id = "cookie-trace".to_string();
    let response = browser.fetch(second).await.unwrap();
    assert!(String::from_utf8_lossy(response.body()).contains("cookie-reused"));

    browser.close().await.unwrap();
    server.join().unwrap();
}

#[tokio::test]
async fn browser_does_not_forward_request_cookie_to_cross_origin_redirect() {
    if std::env::var_os("BROWSER_TESTS").is_none() {
        return;
    }

    let (source_url, captured, source, target) = cross_origin_redirect_pages();
    let browser = Browser::new();
    browser.open().await.unwrap();
    let request = Request::follow(source_url)
        .unwrap()
        .mode(Mode::Browser)
        .cookie("session", "secret")
        .unwrap();
    let error = browser.fetch(request).await.unwrap_err();

    assert!(matches!(
        error,
        spider::downloader::Error::InvalidConfig(message)
            if message.contains("cross-origin redirect")
    ));
    let captured = captured.lock().unwrap().to_ascii_lowercase();
    assert!(!captured.contains("cookie: session=secret"));
    browser.close().await.unwrap();
    source.join().unwrap();
    target.join().unwrap();
}
