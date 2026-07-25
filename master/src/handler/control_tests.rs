use axum::body::to_bytes;
use axum::extract::Query;
use axum::http::Uri;
use axum::response::IntoResponse as _;

use super::response::{bounded, found};
use crate::types::{item, request, task, trace, worker};

#[test]
fn query_records_reject_unknown_fields() {
    for uri in [
        "/v1/control/tasks?unknown=1",
        "/v1/control/traces?unknown=1",
        "/v1/control/requests?unknown=1",
        "/v1/control/workers?unknown=1",
        "/v1/control/items?unknown=1",
    ] {
        let uri: Uri = uri.parse().unwrap();
        let rejected = if uri.path().ends_with("tasks") {
            Query::<task::List>::try_from_uri(&uri).is_err()
        } else if uri.path().ends_with("traces") {
            Query::<trace::List>::try_from_uri(&uri).is_err()
        } else if uri.path().ends_with("requests") {
            Query::<request::List>::try_from_uri(&uri).is_err()
        } else if uri.path().ends_with("workers") {
            Query::<worker::List>::try_from_uri(&uri).is_err()
        } else {
            Query::<item::List>::try_from_uri(&uri).is_err()
        };
        assert!(rejected, "unknown query field was accepted: {uri}");
    }
}

#[test]
fn query_records_accept_only_documented_filters() {
    let task: Uri = "/v1/control/tasks?limit=1&state=scheduled".parse().unwrap();
    let request: Uri = "/v1/control/requests?trace_id=trace-1&state=pending&worker_id=worker-1"
        .parse()
        .unwrap();
    let worker: Uri = "/v1/control/workers?mode=http&online=true".parse().unwrap();

    assert!(Query::<task::List>::try_from_uri(&task).is_ok());
    assert!(Query::<request::List>::try_from_uri(&request).is_ok());
    assert!(Query::<worker::List>::try_from_uri(&worker).is_ok());
}

#[test]
fn response_limit_is_checked_after_serializing_the_envelope() {
    let value = crate::types::Page {
        items: vec!["123456"],
        next_cursor: None,
    };

    assert!(bounded(value, 8).is_err());
}

#[tokio::test]
async fn task_and_item_absence_return_resource_not_found() {
    for (kind, id) in [("Task", "task-1"), ("Item", "row-1")] {
        let error = found::<()>(None, kind, id.to_string()).unwrap_err();
        let response = error.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: crate::error::ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.error.code, "not_found");
        assert_eq!(body.error.id.as_deref(), Some(id));
        assert_eq!(body.error.message, format!("{kind} not found"));
    }
}

#[tokio::test]
async fn actual_control_routes_reject_unknown_queries_without_a_database() {
    let config = crate::Config::new(
        "127.0.0.1:0".parse().unwrap(),
        "mysql://crawler",
        "crawler",
        "worker-secret",
        "control-secret",
    )
    .unwrap();
    let store = crate::store::MySql::disconnected(&config);
    let app = super::build(config, store);

    for path in [
        "/v1/control/tasks?unknown=1",
        "/v1/control/traces?unknown=1",
        "/v1/control/requests?unknown=1",
        "/v1/control/workers?unknown=1",
        "/v1/control/items?unknown=1",
    ] {
        let response = super::tests::request(
            app.clone(),
            format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer control-secret\r\nX-Crawler-Namespace: crawler\r\n\r\n"
            ),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "{path}: {response}");
        assert!(response.contains("\"code\":\"invalid_request\""));
    }
}

#[tokio::test]
async fn actual_detail_routes_validate_row_ids_without_a_database() {
    let config = crate::Config::new(
        "127.0.0.1:0".parse().unwrap(),
        "mysql://crawler",
        "crawler",
        "worker-secret",
        "control-secret",
    )
    .unwrap();
    let store = crate::store::MySql::disconnected(&config);
    let app = super::build(config, store);
    let invalid = "x".repeat(192);

    for resource in ["tasks", "traces", "requests", "items"] {
        let response = super::tests::request(
            app.clone(),
            format!(
                "GET /v1/control/{resource}/{invalid} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer control-secret\r\nX-Crawler-Namespace: crawler\r\n\r\n"
            ),
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "{resource}: {response}"
        );
    }
}
