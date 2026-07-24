use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_openai::error::OpenAIError;
use async_openai::middleware::HttpRequestFactory;
use bytes::BytesMut;
use futures_util::StreamExt;
use reqwest::ResponseBuilderExt;

pub(super) const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub(super) struct Service {
    client: reqwest::Client,
}

impl Service {
    pub(super) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl tower::Service<HttpRequestFactory> for Service {
    type Response = reqwest::Response;
    type Error = OpenAIError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequestFactory) -> Self::Future {
        let client = self.client.clone();
        Box::pin(async move {
            let request = request.build().await?;
            let response = client
                .execute(request)
                .await
                .map_err(OpenAIError::Reqwest)?;
            read(response, MAX_BODY_BYTES).await
        })
    }
}

async fn read(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<reqwest::Response, OpenAIError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(body_too_large(max_bytes));
    }

    let status = response.status();
    let version = response.version();
    let mut headers = response.headers().clone();
    let url = response.url().clone();
    let capacity = response
        .content_length()
        .unwrap_or_default()
        .min(max_bytes as u64) as usize;
    let mut bytes = BytesMut::with_capacity(capacity);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(OpenAIError::Reqwest)?;
        let length = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| body_too_large(max_bytes))?;
        if length > max_bytes {
            return Err(body_too_large(max_bytes));
        }
        bytes.extend_from_slice(&chunk);
    }

    headers.remove(http::header::CONTENT_ENCODING);
    headers.remove(http::header::CONTENT_LENGTH);
    headers.remove(http::header::TRANSFER_ENCODING);
    let mut response = http::Response::builder()
        .status(status)
        .version(version)
        .url(url)
        .body(bytes.freeze())
        .map_err(|_| {
            OpenAIError::InvalidArgument(
                "failed to rebuild the bounded provider response".to_string(),
            )
        })?;
    *response.headers_mut() = headers;
    Ok(response.into())
}

#[derive(Debug)]
struct BodyTooLarge {
    max_bytes: usize,
}

impl std::fmt::Display for BodyTooLarge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "provider response exceeds the {} byte limit",
            self.max_bytes
        )
    }
}

impl std::error::Error for BodyTooLarge {}

fn body_too_large(max_bytes: usize) -> OpenAIError {
    OpenAIError::Boxed(Box::new(BodyTooLarge { max_bytes }))
}

pub(super) fn body_limit(error: &OpenAIError) -> Option<usize> {
    let OpenAIError::Boxed(error) = error else {
        return None;
    };
    error
        .downcast_ref::<BodyTooLarge>()
        .map(|error| error.max_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(body: impl Into<reqwest::Body>) -> reqwest::Response {
        http::Response::builder()
            .status(200)
            .url(url::Url::parse("https://provider.example/v1").unwrap())
            .body(body)
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn accepts_an_exact_declared_limit_and_rejects_larger_bodies() {
        let accepted = read(response(bytes::Bytes::from_static(b"1234")), 4)
            .await
            .unwrap();
        assert_eq!(accepted.bytes().await.unwrap(), "1234");

        let error = read(response(bytes::Bytes::from_static(b"12345")), 4)
            .await
            .unwrap_err();
        assert_eq!(body_limit(&error), Some(4));
    }

    #[tokio::test]
    async fn rejects_streamed_bodies_without_a_declared_length() {
        let body = reqwest::Body::wrap_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"12")),
            Ok(bytes::Bytes::from_static(b"345")),
        ]));

        let error = read(response(body), 4).await.unwrap_err();

        assert_eq!(body_limit(&error), Some(4));
    }
}
