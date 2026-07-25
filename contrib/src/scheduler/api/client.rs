mod response;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{io, sync::Arc};

use bytes::Bytes;
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use spider::scheduler;

use super::error::Error;

#[cfg(test)]
pub(super) use response::map_error;

const ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(25);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const RETRY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
struct Timeouts {
    connect: Duration,
    read: Duration,
    request: Duration,
    retries: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect: CONNECT_TIMEOUT,
            read: READ_TIMEOUT,
            request: REQUEST_TIMEOUT,
            retries: RETRY_TIMEOUT,
        }
    }
}

#[derive(Clone)]
pub(super) struct Client {
    http: reqwest::Client,
    base_url: url::Url,
    token: String,
    namespace: String,
    max_response_bytes: usize,
    max_request_bytes: Arc<AtomicUsize>,
    retry_timeout: Duration,
}

impl Client {
    pub(super) fn new(
        base_url: url::Url,
        token: String,
        namespace: String,
        max_response_bytes: usize,
    ) -> Result<Self, Error> {
        Self::build(
            base_url,
            token,
            namespace,
            max_response_bytes,
            Timeouts::default(),
        )
    }

    fn build(
        base_url: url::Url,
        token: String,
        namespace: String,
        max_response_bytes: usize,
        timeouts: Timeouts,
    ) -> Result<Self, Error> {
        let http = reqwest::Client::builder()
            .connect_timeout(timeouts.connect)
            .read_timeout(timeouts.read)
            .timeout(timeouts.request)
            .build()?;
        Ok(Self {
            http,
            base_url,
            token,
            namespace,
            max_response_bytes,
            max_request_bytes: Arc::new(AtomicUsize::new(max_response_bytes)),
            retry_timeout: timeouts.retries,
        })
    }

    pub(super) fn with_namespace(&self, namespace: String) -> Self {
        Self {
            namespace,
            ..self.clone()
        }
    }

    pub(super) fn with_max_response_bytes(&self, max_response_bytes: usize) -> Self {
        Self {
            max_response_bytes,
            max_request_bytes: Arc::new(AtomicUsize::new(max_response_bytes)),
            ..self.clone()
        }
    }

    pub(super) fn set_max_request_bytes(&self, max_request_bytes: usize) {
        self.max_request_bytes
            .store(max_request_bytes, Ordering::Release);
    }

    pub(super) fn with_retry_deadline(&self, deadline: Duration) -> Self {
        Self {
            retry_timeout: RETRY_TIMEOUT.min(deadline),
            ..self.clone()
        }
    }

    pub(super) fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }

    pub(super) fn validate_body<T>(&self, body: &T) -> Result<(), scheduler::Error>
    where
        T: Serialize + ?Sized,
    {
        encode(body, self.max_request_bytes.load(Ordering::Acquire)).map(|_| ())
    }

    pub(super) async fn get<T>(&self, path: &str) -> Result<T, scheduler::Error>
    where
        T: DeserializeOwned,
    {
        response::decode(self.send::<()>(Method::GET, path, None, None).await?)
    }

    pub(super) async fn get_segments<T>(&self, segments: &[&str]) -> Result<T, scheduler::Error>
    where
        T: DeserializeOwned,
    {
        let mut url = self.base_url.clone();
        {
            let mut path = url.path_segments_mut().map_err(|()| {
                scheduler::Error::Message(
                    "Master base_url cannot contain path segments".to_string(),
                )
            })?;
            for segment in segments {
                path.push(segment);
            }
        }
        response::decode(self.send_url::<()>(Method::GET, url, None, None).await?)
    }

    pub(super) async fn post<B, T>(
        &self,
        path: &str,
        body: &B,
        operation_key: Option<&str>,
    ) -> Result<T, scheduler::Error>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        response::decode(
            self.send(Method::POST, path, Some(body), operation_key)
                .await?,
        )
    }

    pub(super) async fn post_empty<B>(
        &self,
        path: &str,
        body: &B,
        operation_key: Option<&str>,
    ) -> Result<(), scheduler::Error>
    where
        B: Serialize + ?Sized,
    {
        response::empty(
            self.send(Method::POST, path, Some(body), operation_key)
                .await?,
        )
    }

    async fn send<B>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        operation_key: Option<&str>,
    ) -> Result<Vec<u8>, scheduler::Error>
    where
        B: Serialize + ?Sized,
    {
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        self.send_url(method, url, body, operation_key).await
    }

    async fn send_url<B>(
        &self,
        method: Method,
        url: url::Url,
        body: Option<&B>,
        operation_key: Option<&str>,
    ) -> Result<Vec<u8>, scheduler::Error>
    where
        B: Serialize + ?Sized,
    {
        let body = body
            .map(|body| encode(body, self.max_request_bytes.load(Ordering::Acquire)))
            .transpose()?;
        match tokio::time::timeout(
            self.retry_timeout,
            self.send_attempts(method, url, body, operation_key),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(scheduler::Error::Unavailable(format!(
                "Master request exceeded the {:?} retry deadline",
                self.retry_timeout
            ))),
        }
    }

    async fn send_attempts(
        &self,
        method: Method,
        url: url::Url,
        body: Option<Bytes>,
        operation_key: Option<&str>,
    ) -> Result<Vec<u8>, scheduler::Error> {
        let mut last = None;

        for attempt in 0..ATTEMPTS {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .bearer_auth(&self.token)
                .header("X-Crawler-Namespace", &self.namespace);
            if let Some(operation_key) = operation_key {
                request = request.header("Idempotency-Key", operation_key);
            }
            if let Some(body) = &body {
                request = request
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body.clone());
            }

            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    last = Some(error.to_string());
                    if attempt + 1 < ATTEMPTS {
                        retry(attempt).await;
                        continue;
                    }
                    break;
                }
            };
            let status = response.status();
            let bytes = match response::read(response, self.max_response_bytes).await {
                Ok(bytes) => bytes,
                Err(response::ReadError::Transport(error)) => {
                    last = Some(error);
                    if attempt + 1 < ATTEMPTS {
                        retry(attempt).await;
                        continue;
                    }
                    break;
                }
                Err(response::ReadError::TooLarge(message)) => {
                    if retryable(status) {
                        last = Some(message);
                        if attempt + 1 < ATTEMPTS {
                            retry(attempt).await;
                            continue;
                        }
                        break;
                    }
                    return if status.is_success() && method == Method::POST {
                        Err(scheduler::Error::Unavailable(format!(
                            "{message}; successful mutation outcome is ambiguous"
                        )))
                    } else {
                        Err(scheduler::Error::Message(message))
                    };
                }
            };

            if status.is_success() {
                return Ok(bytes);
            }
            if retryable(status) {
                last = Some(response::message(status, &bytes));
                if attempt + 1 < ATTEMPTS {
                    retry(attempt).await;
                    continue;
                }
                break;
            }
            return Err(response::map_error(status, &bytes));
        }

        Err(scheduler::Error::Unavailable(last.unwrap_or_else(|| {
            "Master request failed without a response".to_string()
        })))
    }
}

fn encode<T>(value: &T, limit: usize) -> Result<Bytes, scheduler::Error>
where
    T: Serialize + ?Sized,
{
    let mut writer = LimitWriter::new(limit);
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        return if writer.exceeded {
            Err(scheduler::Error::Message(format!(
                "Master request exceeds the configured {limit} byte limit"
            )))
        } else {
            Err(scheduler::Error::Message(error.to_string()))
        };
    }
    Ok(Bytes::from(writer.bytes))
}

struct LimitWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl LimitWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }
}

impl io::Write for LimitWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("serialized request exceeded its limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn retry(attempt: usize) {
    let multiplier = u32::try_from(attempt + 1).unwrap_or(u32::MAX);
    tokio::time::sleep(RETRY_DELAY.saturating_mul(multiplier)).await;
}

fn retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde_json::Value;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[test]
    fn lease_deadline_only_tightens_the_default_retry_window() {
        let client = client(
            "https://master.example.com/".to_string(),
            Timeouts::default(),
        );

        assert_eq!(
            client
                .with_retry_deadline(Duration::from_secs(2))
                .retry_timeout,
            Duration::from_secs(2)
        );
        assert_eq!(
            client
                .with_retry_deadline(Duration::from_secs(20))
                .retry_timeout,
            RETRY_TIMEOUT
        );
    }

    fn client(base_url: String, timeouts: Timeouts) -> Client {
        Client::build(
            url::Url::parse(&base_url).unwrap(),
            "token".to_string(),
            "default".to_string(),
            1024,
            timeouts,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn retry_deadline_bounds_a_server_that_never_responds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _stream = stream;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                });
            }
        });
        let client = client(
            format!("http://{address}/"),
            Timeouts {
                connect: Duration::from_millis(20),
                read: Duration::from_secs(1),
                request: Duration::from_secs(1),
                retries: Duration::from_millis(80),
            },
        );

        let started = Instant::now();
        let result = client.get::<Value>("stalled").await;
        let elapsed = started.elapsed();
        server.abort();

        assert!(matches!(result, Err(scheduler::Error::Unavailable(_))));
        assert!(elapsed < Duration::from_millis(500), "elapsed: {elapsed:?}");
    }

    #[tokio::test]
    async fn read_timeout_bounds_a_response_body_that_stalls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut request = [0; 1024];
                    let _ = stream.read(&mut request).await;
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\nConnection: close\r\n\r\n{",
                        )
                        .await;
                    let _ = stream.flush().await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                });
            }
        });
        let client = client(
            format!("http://{address}/"),
            Timeouts {
                connect: Duration::from_millis(20),
                read: Duration::from_millis(20),
                request: Duration::from_millis(60),
                retries: Duration::from_millis(180),
            },
        );

        let started = Instant::now();
        let result = client.get::<Value>("stalled-body").await;
        let elapsed = started.elapsed();
        server.abort();

        assert!(matches!(result, Err(scheduler::Error::Unavailable(_))));
        assert!(elapsed < Duration::from_millis(500), "elapsed: {elapsed:?}");
    }

    #[tokio::test]
    async fn oversized_request_is_rejected_before_network_io() {
        let client = client("http://127.0.0.1:9/".to_string(), Timeouts::default());
        let result = client
            .post_empty(
                "items",
                &serde_json::json!({"value": "x".repeat(2048)}),
                None,
            )
            .await;

        assert!(
            matches!(result, Err(scheduler::Error::Message(message)) if message.contains("request exceeds"))
        );
    }

    #[tokio::test]
    async fn oversized_successful_post_is_ambiguous_but_get_is_not() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut request = [0; 1024];
                    let _ = stream.read(&mut request).await;
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 1025\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                });
            }
        });
        let client = client(
            format!("http://{address}/"),
            Timeouts {
                connect: Duration::from_millis(20),
                read: Duration::from_secs(1),
                request: Duration::from_secs(1),
                retries: Duration::from_secs(1),
            },
        );

        let post = client.post_empty("mutation", &(), None).await;
        let get = client.get::<Value>("read").await;
        server.await.unwrap();

        assert!(matches!(post, Err(scheduler::Error::Unavailable(_))));
        assert!(matches!(get, Err(scheduler::Error::Message(_))));
    }
}
