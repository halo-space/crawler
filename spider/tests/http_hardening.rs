use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use spider::downloader::{self, Download};
use spider::net::{Request, Response};

#[macros::spider]
struct RetrySpider {
    url: String,
    parses: Arc<AtomicUsize>,
}

#[macros::spider]
impl RetrySpider {
    fn name(&self) -> &str {
        "http-retry"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let mut request = Request::follow(&self.url)
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .with_retry(1, [0]);
        request.timeout = Some(80);
        self.tx.request(vec![request]).await
    }

    async fn index(&self, _response: Response) -> Result<(), spider::Error> {
        self.parses.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

fn read_request(stream: &mut TcpStream) {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 256];
    while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
        let size = stream.read(&mut chunk).unwrap();
        if size == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..size]);
    }
}

#[test]
fn worker_body_limit_must_be_positive() {
    assert!(matches!(
        downloader::http::Http::new().with_max_body_bytes(0),
        Err(downloader::Error::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn request_cannot_raise_the_worker_body_limit() {
    let (listener, url) = listener();
    listener.set_nonblocking(true).unwrap();
    let request = Request::follow(url).unwrap().max_body_bytes(9);
    let http = downloader::http::Http::new()
        .with_max_body_bytes(8)
        .unwrap();

    let error = http.fetch(request).await.unwrap_err();

    assert!(matches!(error, downloader::Error::InvalidConfig(_)));
    assert!(matches!(
        listener.accept(),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
}

#[tokio::test]
async fn request_can_lower_the_worker_body_limit() {
    let (listener, url) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789",
            )
            .unwrap();
    });
    let request = Request::follow(url).unwrap().max_body_bytes(8);
    let http = downloader::http::Http::new()
        .with_max_body_bytes(16)
        .unwrap();

    let error = http.fetch(request).await.unwrap_err();
    server.join().unwrap();

    assert!(matches!(
        error,
        downloader::Error::BodyTooLarge { limit: 8 }
    ));
}

#[tokio::test]
async fn response_preserves_repeated_and_opaque_header_values() {
    let (listener, url) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nLink: </one>\r\nLink: </two>\r\nX-Opaque: \xFF\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            )
            .unwrap();
    });

    let response = downloader::http::Http::new()
        .fetch(Request::follow(url).unwrap())
        .await
        .unwrap();
    server.join().unwrap();

    assert_eq!(
        response
            .headers
            .get_all("link")
            .iter()
            .map(|value| value.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["</one>", "</two>"]
    );
    assert_eq!(
        response.headers.get("x-opaque").unwrap().as_bytes(),
        b"\xFF"
    );
}

#[tokio::test]
async fn reqwest_errors_do_not_expose_url_credentials_or_query_values() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let request = Request::follow(format!(
        "http://request-user:request-password@{address}/private?api_key=url-secret"
    ))
    .unwrap();

    let error = downloader::http::Http::new()
        .fetch(request)
        .await
        .unwrap_err();
    let message = error.to_string();

    for secret in ["request-user", "request-password", "api_key", "url-secret"] {
        assert!(
            !message.contains(secret),
            "download error exposed {secret}: {message}"
        );
    }
}

#[tokio::test]
async fn omitted_request_limit_uses_the_worker_body_limit() {
    let (listener, url) = listener();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789",
            )
            .unwrap();
    });
    let request = Request::follow(url).unwrap();
    let http = downloader::http::Http::new()
        .with_max_body_bytes(8)
        .unwrap();

    let error = http.fetch(request).await.unwrap_err();
    server.join().unwrap();

    assert!(matches!(
        error,
        downloader::Error::BodyTooLarge { limit: 8 }
    ));
}

#[tokio::test]
async fn redirects_and_body_streaming_share_one_timeout_budget() {
    let (listener, base_url) = listener();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        thread::sleep(Duration::from_millis(150));
        let _ = first.write_all(
            b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );

        let (mut second, _) = listener.accept().unwrap();
        read_request(&mut second);
        let _ =
            second.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n");
        thread::sleep(Duration::from_millis(150));
        let _ = second.write_all(b"ok");
    });
    let mut request = Request::follow(format!("{base_url}/start")).unwrap();
    request.timeout = Some(240);

    let error = downloader::http::Http::new()
        .fetch(request)
        .await
        .unwrap_err();
    server.join().unwrap();

    assert!(matches!(error, downloader::Error::Timeout));
}

#[tokio::test]
async fn each_fetch_receives_a_fresh_timeout_budget() {
    let (listener, url) = listener();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        thread::sleep(Duration::from_millis(120));
        let _ =
            first.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");

        let (mut second, _) = listener.accept().unwrap();
        read_request(&mut second);
        let _ = second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    });
    let mut request = Request::follow(url).unwrap();
    request.timeout = Some(80);
    let http = downloader::http::Http::new();

    let first = http.fetch(request.clone()).await.unwrap_err();
    let second = http.fetch(request).await.unwrap();
    server.join().unwrap();

    assert!(matches!(first, downloader::Error::Timeout));
    assert_eq!(second.body.as_ref(), b"ok");
}

#[tokio::test]
async fn engine_download_retry_starts_with_a_fresh_timeout_budget() {
    let (listener, url) = listener();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        read_request(&mut first);
        let delayed = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            let _ = first.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nlate",
            );
        });

        let (mut second, _) = listener.accept().unwrap();
        read_request(&mut second);
        second
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .unwrap();
        delayed.join().unwrap();
    });
    let parses = Arc::new(AtomicUsize::new(0));
    let mut engine = spider::engine::Builder::new()
        .with_spider(RetrySpider::new(url, Arc::clone(&parses)))
        .build()
        .with_concurrency(1);

    engine.start().await.unwrap();
    server.join().unwrap();

    assert_eq!(parses.load(Ordering::SeqCst), 1);
}
