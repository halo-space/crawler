use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use flate2::Compression;
use flate2::write::GzEncoder;

pub(crate) struct Reply {
    status: u16,
    reason: &'static str,
    body: Vec<u8>,
    content_length: Option<usize>,
    content_encoding: Option<&'static str>,
}

impl Reply {
    pub(crate) fn completion(content: Option<&str>) -> Self {
        Self {
            status: 200,
            reason: "OK",
            body: serde_json::json!({
                "id": "chatcmpl-test",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content,
                    },
                    "finish_reason": "stop"
                }],
                "created": 0,
                "model": "test-model",
                "object": "chat.completion",
                "usage": null
            })
            .to_string()
            .into_bytes(),
            content_length: None,
            content_encoding: None,
        }
    }

    pub(crate) fn error(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            reason: reason(status),
            body: body.into().into_bytes(),
            content_length: None,
            content_encoding: None,
        }
    }

    pub(crate) fn declared_length(status: u16, content_length: usize) -> Self {
        Self {
            status,
            reason: reason(status),
            body: b"{}".to_vec(),
            content_length: Some(content_length),
            content_encoding: None,
        }
    }

    pub(crate) fn gzip(status: u16, body: &[u8]) -> Self {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body).unwrap();
        Self {
            status,
            reason: reason(status),
            body: encoder.finish().unwrap(),
            content_length: None,
            content_encoding: Some("gzip"),
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

pub(crate) struct Server {
    base_url: String,
    requests: Receiver<String>,
    count: Arc<AtomicUsize>,
    stop: Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl Server {
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn request(&self, timeout: Duration) -> Result<String, RecvTimeoutError> {
        self.requests.recv_timeout(timeout)
    }

    pub(crate) fn request_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn server(content: Option<&str>) -> (String, Receiver<String>) {
    let reply = Reply::completion(content);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = channel();
    let (_, stop) = channel();
    let count = Arc::new(AtomicUsize::new(0));
    std::thread::spawn(move || {
        serve(
            listener,
            VecDeque::from([reply]),
            sender,
            count,
            stop,
            Some(Instant::now() + Duration::from_secs(5)),
        );
    });
    (format!("http://{address}"), receiver)
}

pub(crate) fn server_with(replies: Vec<Reply>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_sender, requests) = channel();
    let (stop, stop_receiver) = channel();
    let count = Arc::new(AtomicUsize::new(0));
    let server_count = Arc::clone(&count);
    let thread = std::thread::spawn(move || {
        serve(
            listener,
            replies.into(),
            request_sender,
            server_count,
            stop_receiver,
            None,
        );
    });
    Server {
        base_url: format!("http://{address}"),
        requests,
        count,
        stop,
        thread: Some(thread),
    }
}

pub(crate) fn server_after_all_requests(replies: Vec<Reply>) -> Server {
    assert!(!replies.is_empty());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_sender, requests) = channel();
    let (stop, stop_receiver) = channel();
    let count = Arc::new(AtomicUsize::new(0));
    let server_count = Arc::clone(&count);
    let thread = std::thread::spawn(move || {
        serve_after_all_requests(
            listener,
            replies.into(),
            request_sender,
            server_count,
            stop_receiver,
        );
    });
    Server {
        base_url: format!("http://{address}"),
        requests,
        count,
        stop,
        thread: Some(thread),
    }
}

fn serve(
    listener: TcpListener,
    mut replies: VecDeque<Reply>,
    requests: Sender<String>,
    count: Arc<AtomicUsize>,
    stop: Receiver<()>,
    deadline: Option<Instant>,
) {
    listener.set_nonblocking(true).unwrap();
    while !replies.is_empty() {
        if stop.try_recv().is_ok() || deadline.is_some_and(|limit| Instant::now() >= limit) {
            break;
        }
        let (mut stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(_) => break,
        };
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let Ok(request) = read_request(&mut stream) else {
            continue;
        };
        count.fetch_add(1, Ordering::SeqCst);
        let _ = requests.send(request);
        let reply = replies.pop_front().unwrap();
        if write_reply(&mut stream, &reply).is_err() {
            break;
        }
    }
}

fn serve_after_all_requests(
    listener: TcpListener,
    replies: VecDeque<Reply>,
    requests: Sender<String>,
    count: Arc<AtomicUsize>,
    stop: Receiver<()>,
) {
    listener.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut pending = Vec::with_capacity(replies.len());
    for reply in replies {
        loop {
            if stop.try_recv().is_ok() || Instant::now() >= deadline {
                return;
            }
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(_) => return,
            };
            let _ = stream.set_nonblocking(false);
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
            let Ok(request) = read_request(&mut stream) else {
                continue;
            };
            count.fetch_add(1, Ordering::SeqCst);
            let _ = requests.send(request);
            pending.push((stream, reply));
            break;
        }
    }

    for (mut stream, reply) in pending {
        if write_reply(&mut stream, &reply).is_err() {
            return;
        }
    }
}

fn write_reply(stream: &mut std::net::TcpStream, reply: &Reply) -> std::io::Result<()> {
    let content_length = reply.content_length.unwrap_or(reply.body.len());
    write!(
        stream,
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        reply.status, reply.reason, content_length
    )?;
    if let Some(encoding) = reply.content_encoding {
        write!(stream, "Content-Encoding: {encoding}\r\n")?;
    }
    write!(stream, "Connection: close\r\n\r\n")?;
    stream.write_all(&reply.body)
}

fn read_request(stream: &mut std::net::TcpStream) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .unwrap_or_default();
    while bytes.len() < header_end + content_length {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    String::from_utf8(bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}
