use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use spider::downloader::Download;
use spider::downloader::browser::Browser;
use spider::net::{Mode, Request};

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

    let request = Request::follow(url)
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
    server.join().unwrap();
}
