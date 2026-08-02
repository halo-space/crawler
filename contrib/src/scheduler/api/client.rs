mod response;

use std::time::Duration;

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
    retry_timeout: Duration,
}

impl Client {
    pub(super) fn new(base_url: url::Url, token: String, namespace: String) -> Result<Self, Error> {
        Self::build(base_url, token, namespace, Timeouts::default())
    }

    fn build(
        base_url: url::Url,
        token: String,
        namespace: String,
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
            retry_timeout: timeouts.retries,
        })
    }

    pub(super) fn with_namespace(&self, namespace: String) -> Self {
        Self {
            namespace,
            ..self.clone()
        }
    }

    pub(super) fn with_retry_deadline(&self, deadline: Duration) -> Self {
        Self {
            retry_timeout: RETRY_TIMEOUT.min(deadline),
            ..self.clone()
        }
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

    pub(super) async fn post_operation<B, T>(
        &self,
        path: &str,
        body: &B,
        operation_key: &str,
    ) -> Result<T, scheduler::Error>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        response::operation(
            self.send(Method::POST, path, Some(body), Some(operation_key))
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

    pub(super) async fn post_once<B, T>(&self, path: &str, body: &B) -> Result<T, scheduler::Error>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        response::decode(
            self.send_with_attempts(Method::POST, path, Some(body), None, 1)
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
        self.send_with_attempts(method, path, body, operation_key, ATTEMPTS)
            .await
    }

    async fn send_with_attempts<B>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        operation_key: Option<&str>,
        attempts: usize,
    ) -> Result<Vec<u8>, scheduler::Error>
    where
        B: Serialize + ?Sized,
    {
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        self.send_url_with_attempts(method, url, body, operation_key, attempts)
            .await
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
        self.send_url_with_attempts(method, url, body, operation_key, ATTEMPTS)
            .await
    }

    async fn send_url_with_attempts<B>(
        &self,
        method: Method,
        url: url::Url,
        body: Option<&B>,
        operation_key: Option<&str>,
        attempts: usize,
    ) -> Result<Vec<u8>, scheduler::Error>
    where
        B: Serialize + ?Sized,
    {
        let body = body.map(encode).transpose()?;
        match tokio::time::timeout(
            self.retry_timeout,
            self.send_attempts(method, url, body, operation_key, attempts),
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
        attempts: usize,
    ) -> Result<Vec<u8>, scheduler::Error> {
        let mut last = None;

        for attempt in 0..attempts {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .bearer_auth(&self.token)
                .header("X-Crawler-Namespace", &self.namespace);
            #[cfg(feature = "runtime-tracing")]
            if let Some(traceparent) = traceparent() {
                request = request.header("traceparent", traceparent);
            }
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
                    if attempt + 1 < attempts {
                        retry(attempt).await;
                        continue;
                    }
                    break;
                }
            };
            let status = response.status();
            let bytes = match response::read(response).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    last = Some(error);
                    if attempt + 1 < attempts {
                        retry(attempt).await;
                        continue;
                    }
                    break;
                }
            };

            if status.is_success() {
                return Ok(bytes);
            }
            if retryable(status) {
                last = Some(response::message(status, &bytes));
                if attempt + 1 < attempts {
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

fn encode<T>(value: &T) -> Result<Bytes, scheduler::Error>
where
    T: Serialize + ?Sized,
{
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| scheduler::Error::Message(error.to_string()))
}

async fn retry(attempt: usize) {
    let multiplier = u32::try_from(attempt + 1).unwrap_or(u32::MAX);
    tokio::time::sleep(RETRY_DELAY.saturating_mul(multiplier)).await;
}

fn retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[cfg(feature = "runtime-tracing")]
fn traceparent() -> Option<String> {
    fastrace::collector::SpanContext::current_local_parent()
        .map(|context| context.encode_w3c_traceparent())
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
    async fn reads_a_large_master_response_without_a_size_policy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let value = "x".repeat(2048);
        let body = serde_json::to_vec(&value).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).await;
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
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

        let response = client.get::<String>("read").await.unwrap();
        server.await.unwrap();

        assert_eq!(response, value);
    }
}
