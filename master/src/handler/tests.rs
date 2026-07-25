use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, FromRequestParts as _, rejection::JsonRejection};
use axum::http::{HeaderMap, Request, header, request::Parts};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::oneshot;

use crate::Config;
use crate::store::MySql;
use crate::svc::Context;

use super::*;

fn config() -> Config {
    Config::new(
        "127.0.0.1:0".parse().unwrap(),
        "mysql://crawler",
        "crawler",
        "worker-secret",
        "control-secret",
    )
    .unwrap()
}

fn headers(token: &str, namespace: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    headers.insert("X-Crawler-Namespace", namespace.parse().unwrap());
    headers
}

fn parts(token: &str, namespace: &str) -> Parts {
    let (mut parts, _) = Request::new(()).into_parts();
    parts.headers = headers(token, namespace);
    parts
}

#[tokio::test]
async fn authentication_extractors_isolate_tokens_and_namespace() {
    let config = config();
    let store = MySql::disconnected(&config);
    let app = Context {
        config: Arc::new(config),
        store,
    };

    let mut worker = parts("worker-secret", "crawler");
    assert!(
        access::Worker::from_request_parts(&mut worker, &app)
            .await
            .is_ok()
    );

    let mut wrong_worker = parts("control-secret", "crawler");
    assert!(matches!(
        access::Worker::from_request_parts(&mut wrong_worker, &app).await,
        Err(crate::Error::Unauthorized)
    ));

    let mut wrong_namespace = parts("worker-secret", "other");
    assert!(matches!(
        access::Worker::from_request_parts(&mut wrong_namespace, &app).await,
        Err(crate::Error::Unauthorized)
    ));

    let mut control = parts("control-secret", "crawler");
    assert!(
        access::Control::from_request_parts(&mut control, &app)
            .await
            .is_ok()
    );

    let mut wrong_control = parts("worker-secret", "crawler");
    assert!(matches!(
        access::Control::from_request_parts(&mut wrong_control, &app).await,
        Err(crate::Error::Unauthorized)
    ));
}

#[test]
fn idempotency_requires_a_non_empty_header() {
    let mut headers = HeaderMap::new();
    assert!(extract::operation(&headers).is_err());
    headers.insert("Idempotency-Key", " ".parse().unwrap());
    assert!(extract::operation(&headers).is_err());
    headers.insert("Idempotency-Key", "operation-1".parse().unwrap());
    assert_eq!(extract::operation(&headers).unwrap(), "operation-1");
}

#[tokio::test]
async fn body_limit_and_json_rejections_use_the_machine_envelope() {
    async fn bounded(
        body: Result<Json<Value>, JsonRejection>,
    ) -> Result<Json<Value>, crate::Error> {
        let _ = extract::json(body)?;
        Ok(response::empty())
    }

    let app = Router::new()
        .route("/", post(bounded))
        .layer(DefaultBodyLimit::max(16));
    let body = "{\"value\":\"this exceeds sixteen bytes\"}";
    let response = request(
        app,
        format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    assert!(
        response.contains("\"code\":\"invalid_request\""),
        "{response}"
    );
    assert!(response.contains("request body exceeds the configured limit"));
}

#[tokio::test]
async fn authentication_precedes_body_extraction_on_worker_and_control_routes() {
    let config = config().with_max_api_bytes(16).unwrap();
    let store = MySql::disconnected(&config);
    let app = build(config, store);
    let body = "{\"value\": this malformed body exceeds sixteen bytes}";

    for (method, path, token, wrong_token) in [
        (
            "POST",
            "/v1/worker/requests/push",
            "worker-secret",
            "control-secret",
        ),
        (
            "PUT",
            "/v1/control/tasks/task-1",
            "control-secret",
            "worker-secret",
        ),
    ] {
        for credentials in [
            String::new(),
            format!("Authorization: Bearer {wrong_token}\r\nX-Crawler-Namespace: crawler\r\n"),
            format!("Authorization: Bearer {token}\r\nX-Crawler-Namespace: other\r\n"),
        ] {
            let response = request(
                app.clone(),
                format!(
                    "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{credentials}Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                ),
            )
            .await;

            assert!(response.starts_with("HTTP/1.1 401"), "{path}: {response}");
            assert!(response.contains("\"code\":\"unauthorized\""), "{response}");
            assert!(!response.contains("\"code\":\"invalid_request\""));
        }
    }
}

pub(super) async fn request(app: Router, request: String) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let _ = stop.send(());
    server.await.unwrap();

    String::from_utf8(response).unwrap()
}
