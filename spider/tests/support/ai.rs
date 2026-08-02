use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Clone, Debug)]
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: Value,
}

pub(crate) struct Server {
    base_url: String,
    expected: usize,
    requests: Receiver<Result<Request, String>>,
    stop: Sender<()>,
    thread: Option<JoinHandle<Result<(), String>>>,
}

impl Server {
    pub(crate) fn start(outputs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let outputs = outputs.into_iter().map(Into::into).collect::<Vec<_>>();
        assert!(
            !outputs.is_empty(),
            "AI mock requires at least one response"
        );
        let expected = outputs.len();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind AI mock server");
        listener
            .set_nonblocking(true)
            .expect("set AI mock listener nonblocking");
        let address = listener.local_addr().expect("read AI mock address");
        let (request_sender, requests) = mpsc::channel();
        let (stop, stop_receiver) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            for output in outputs {
                let mut stream = accept(&listener, &stop_receiver)?;
                stream
                    .set_nonblocking(false)
                    .map_err(|error| error.to_string())?;
                stream
                    .set_read_timeout(Some(IO_TIMEOUT))
                    .map_err(|error| error.to_string())?;
                stream
                    .set_write_timeout(Some(IO_TIMEOUT))
                    .map_err(|error| error.to_string())?;
                let request = read(&mut stream);
                let readable = request.as_ref().cloned().map_err(Clone::clone);
                request_sender
                    .send(readable)
                    .map_err(|error| error.to_string())?;
                request?;
                respond(&mut stream, &output)?;
            }
            Ok(())
        });

        Self {
            base_url: format!("http://{address}/v1"),
            expected,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn finish(mut self) -> Vec<Request> {
        let requests = (0..self.expected)
            .map(|_| {
                self.requests
                    .recv_timeout(IO_TIMEOUT)
                    .expect("AI provider did not receive the expected request")
                    .expect("AI provider received an invalid HTTP request")
            })
            .collect();
        self.join().expect("AI mock server failed");
        requests
    }

    fn join(&mut self) -> Result<(), String> {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() {
            return thread
                .join()
                .map_err(|_| "AI mock server thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.join();
    }
}

fn accept(listener: &TcpListener, stop: &Receiver<()>) -> Result<TcpStream, String> {
    let deadline = Instant::now() + IO_TIMEOUT;
    loop {
        if stop.try_recv().is_ok() {
            return Err("AI mock server stopped before receiving all requests".to_string());
        }
        match listener.accept() {
            Ok((stream, _)) => return Ok(stream),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err("timed out waiting for AI provider request".to_string());
                }
                std::thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn read(stream: &mut TcpStream) -> Result<Request, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("AI provider request ended before its headers".to_string());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header_text =
        std::str::from_utf8(&bytes[..header_end]).map_err(|error| error.to_string())?;
    let mut lines = header_text.split("\r\n");
    let start = lines
        .next()
        .ok_or_else(|| "AI provider request has no request line".to_string())?;
    let mut start = start.split_whitespace();
    let method = start
        .next()
        .ok_or_else(|| "AI provider request has no method".to_string())?
        .to_string();
    let path = start
        .next()
        .ok_or_else(|| "AI provider request has no path".to_string())?
        .to_string();
    let headers = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| format!("invalid AI provider header: {line}"))?;
            Ok((name.to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect::<Result<HashMap<_, _>, String>>()?;
    let content_length = headers
        .get("content-length")
        .ok_or_else(|| "AI provider request has no content-length".to_string())?
        .parse::<usize>()
        .map_err(|error| error.to_string())?;
    while bytes.len() < header_end + content_length {
        let count = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if count == 0 {
            return Err("AI provider request ended before its body".to_string());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let body = serde_json::from_slice(&bytes[header_end..header_end + content_length])
        .map_err(|error| error.to_string())?;
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

fn respond(stream: &mut TcpStream, output: &str) -> Result<(), String> {
    let body = serde_json::json!({
        "id": "chatcmpl-engine-test",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": output},
            "finish_reason": "stop"
        }],
        "created": 0,
        "model": "mock-model",
        "object": "chat.completion",
        "usage": null
    })
    .to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .map_err(|error| error.to_string())
}
