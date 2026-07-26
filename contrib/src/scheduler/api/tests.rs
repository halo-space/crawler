use std::time::Duration;
use std::{collections::HashMap, io::Read as _, io::Write as _, sync::Arc};

use serde_json::json;
use spider::scheduler::Init as _;
use spider::{Scheduler as _, net, payload, scheduler, trace};

use super::Api;
use super::client;
use super::state::{Action, OPERATION_CAPACITY, OPERATION_TTL, Operation, Operations, TraceCache};
use super::wire;
use super::worker::canonical_modes;

mod config;
mod idempotency;
mod lifecycle;
mod operation;
mod recovery;
mod transport;

fn claimed(snapshot: net::request::Snapshot) -> serde_json::Value {
    let trace = trace::Snapshot::code(&snapshot.task_id);
    json!({
        "snapshot": snapshot,
        "execution": {
            "version": 1,
            "next_time": 0,
            "leased_by": "worker-1",
            "lease_time": 1,
            "retry_count": 0,
            "failed_workers": []
        },
        "trace": trace
    })
}

fn bound_request(url: impl Into<String>) -> net::Request {
    let mut request = net::Request::follow(url).unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();
    request
}

fn unavailable(message: &str) -> Response {
    Response::json(
        "503 Service Unavailable",
        json!({"error": {
            "code": "unavailable",
            "id": null,
            "field": null,
            "message": message
        }}),
    )
}

fn operation_keys(requests: &[Request], path: &str) -> Vec<String> {
    requests
        .iter()
        .filter(|request| request.path.ends_with(path))
        .map(|request| {
            request
                .headers
                .get("idempotency-key")
                .expect("operation request must carry an idempotency key")
                .clone()
        })
        .collect()
}

struct Response {
    status: &'static str,
    body: Vec<u8>,
    wait: Option<Wait>,
}

struct Wait {
    reached: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

impl Response {
    fn json(status: &'static str, body: serde_json::Value) -> Self {
        Self {
            status,
            body: serde_json::to_vec(&body).unwrap(),
            wait: None,
        }
    }

    fn empty(status: &'static str) -> Self {
        Self {
            status,
            body: Vec::new(),
            wait: None,
        }
    }

    fn held_json(
        status: &'static str,
        body: serde_json::Value,
        reached: std::sync::mpsc::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) -> Self {
        Self {
            status,
            body: serde_json::to_vec(&body).unwrap(),
            wait: Some(Wait { reached, resume }),
        }
    }
}

struct Request {
    path: String,
    headers: HashMap<String, String>,
}

fn server(
    responses: Vec<Response>,
) -> (
    String,
    std::sync::mpsc::Receiver<Vec<Request>>,
    std::thread::JoinHandle<()>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let Response { status, body, wait } = response;
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            requests.push(request);
            if let Some(wait) = wait {
                wait.reached.send(()).unwrap();
                wait.resume.recv_timeout(Duration::from_secs(2)).unwrap();
            }
            write!(
                stream,
                "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                status,
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
            stream.flush().unwrap();
        }
        sender.send(requests).unwrap();
    });
    (format!("http://{address}"), receiver, server)
}

async fn wait_for_request(receiver: std::sync::mpsc::Receiver<()>) {
    tokio::task::spawn_blocking(move || receiver.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();
}

fn read_request(stream: &mut std::net::TcpStream) -> Request {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut bytes = Vec::new();
    let headers_end = loop {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..headers_end]).unwrap();
    let mut lines = headers.split("\r\n");
    let request_line = lines.next().unwrap();
    let path = request_line.split_ascii_whitespace().nth(1).unwrap();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect::<HashMap<_, _>>();
    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_default();
    let received_body = bytes.len() - headers_end;
    if received_body < content_length {
        let mut remaining = vec![0; content_length - received_body];
        stream.read_exact(&mut remaining).unwrap();
    }
    Request {
        path: path.to_string(),
        headers,
    }
}
