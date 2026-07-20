use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use spider::{engine, net};

static TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(serde::Deserialize, serde::Serialize, macros::Item)]
#[serde(deny_unknown_fields)]
struct Book {
    #[serde(skip)]
    state: spider::item::State,
    title: String,
}

#[macros::spider]
struct AcceptanceSpider {
    start_url: String,
}

#[macros::spider]
impl AcceptanceSpider {
    fn name(&self) -> &str {
        "v1-acceptance"
    }

    async fn start_urls(&self) -> Vec<String> {
        vec![self.start_url.clone()]
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        let detail = response
            .follow("/detail")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .node(Self::detail);
        self.tx.request(vec![detail]).await
    }

    async fn detail(&self, response: net::Response) -> Result<(), spider::Error> {
        let soup = response.css()?;
        let title = soup
            .find("h1")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .map(|node| node.text())
            .ok_or_else(|| spider::Error::Message("missing title".to_string()))?;
        self.tx
            .item(vec![Book {
                state: spider::item::State::default(),
                title,
            }])
            .await
    }
}

#[tokio::test]
async fn default_code_mode_decodes_legacy_http_page_and_writes_jsonl() {
    let (start_url, server) = serve_pages();
    let runtime_dir = temp_dir();
    let mut engine = engine::Builder::new()
        .with_scheduler(spider::Memory::new("worker-1").with_dir(&runtime_dir))
        .with_spider(AcceptanceSpider::new(start_url))
        .build()
        .with_concurrency(2);

    engine.start().await.unwrap();
    server.join().unwrap();

    let trace_ids = engine.scheduler().trace_ids();
    assert_eq!(trace_ids.len(), 1);
    assert!(trace_ids[0].starts_with("trace_v1-acceptance_"));
    let stats = engine.scheduler().trace_stats(&trace_ids[0]);
    assert_eq!(stats["index"].total, 1);
    assert_eq!(stats["index"].done, 1);
    assert_eq!(stats["detail"].total, 1);
    assert_eq!(stats["detail"].done, 1);

    let item_dir = runtime_dir
        .join("data")
        .join("items")
        .join("output")
        .join("v1-acceptance");
    let mut files = tokio::fs::read_dir(item_dir).await.unwrap();
    let item_path = files.next_entry().await.unwrap().unwrap().path();
    assert!(files.next_entry().await.unwrap().is_none());
    let content = tokio::fs::read_to_string(item_path).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(content.trim()).unwrap(),
        serde_json::json!({"title": "桂林米粉"})
    );

    tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
}

fn serve_pages() -> (String, std::thread::JoinHandle<()>) {
    const LIST: &[u8] = b"<html><a href=\"/detail\">Detail</a></html>";
    const DETAIL: &[u8] = b"<!doctype html><html><head><meta charset=\"gbk\"><title>\xB9\xF0\xC1\xD6\xC3\xD7\xB7\xDB</title></head><body><main><article><h1>\xB9\xF0\xC1\xD6\xC3\xD7\xB7\xDB</h1></article></main></body></html>";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 2048];
            let size = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..size]);
            let (body, content_type) = if request.starts_with("GET /detail ") {
                (DETAIL, "text/html; charset=gbk")
            } else {
                (LIST, "text/html; charset=utf-8")
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        }
    });
    (format!("http://{address}/list"), server)
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "crawler-v1-acceptance-{}-{}",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ))
}
