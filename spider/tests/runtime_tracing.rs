#![cfg(feature = "runtime-tracing")]

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use fastrace::collector::SpanRecord;
use spider::{downloader, engine, item, middleware, net, payload};

const SECRET: &str = "runtime-secret-marker";

struct Download;

impl downloader::Download for Download {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        assert!(!request.headers.contains("traceparent"));
        Ok(net::Response::new(
            request,
            net::StatusCode(200),
            Bytes::from_static(b"ok"),
        ))
    }
}

struct Store {
    submissions: AtomicUsize,
}

impl Store {
    fn new() -> Self {
        Self {
            submissions: AtomicUsize::new(0),
        }
    }
}

impl item::Store for Store {
    async fn open(&self) -> Result<(), item::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), item::Error> {
        Ok(())
    }

    async fn submit(&self, payload: &payload::Payload) -> Result<(), item::Error> {
        if self.submissions.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(item::Error::Message(format!(
                "temporary store failure: {SECRET}"
            )));
        }
        payload
            .validate_store()
            .map_err(|error| item::Error::Message(error.to_string()))
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Output {
    value: String,
    #[serde(skip)]
    state: item::State,
    #[serde(skip)]
    middlewares: Vec<middleware::Spec>,
}

impl item::Item for Output {
    fn from_values(values: item::Values) -> Result<Self, item::Error> {
        item::deserialize(values)
    }

    fn state(&self) -> &item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut item::State {
        &mut self.state
    }

    fn middlewares(&self) -> &[middleware::Spec] {
        &self.middlewares
    }
}

#[macros::spider]
struct TraceSpider;

#[macros::spider]
impl TraceSpider {
    fn name(&self) -> &str {
        "runtime-tracing"
    }

    async fn start_urls(&self) -> Vec<String> {
        vec![
            format!("https://example.com/one?token={SECRET}"),
            format!("https://example.com/two?token={SECRET}"),
        ]
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        let retry = middleware::Spec::new("retry")
            .hook("error_item")
            .args(serde_json::json!({"count": 1, "backoff": [0]}));
        self.tx
            .item(vec![Output {
                value: SECRET.to_string(),
                state: item::State::default(),
                middlewares: vec![retry],
            }])
            .await
    }
}

#[tokio::test]
async fn traces_complete_request_lifecycles_without_leaking_content() {
    let (reporter, records) = fastrace::collector::TestReporter::new();
    fastrace::set_reporter(reporter, fastrace::collector::Config::default());

    let mut runtime = engine::Engine::new()
        .with_downloader(Download)
        .with_store(Store::new())
        .with_spider(TraceSpider::new())
        .build()
        .with_concurrency(2)
        .with_tracing(spider::trace::Tracing::all());
    tokio::spawn(async move { runtime.start().await })
        .await
        .unwrap()
        .unwrap();
    fastrace::flush();

    let records = records.lock().clone();
    let roots = records
        .iter()
        .filter(|record| record.name == "crawler.request")
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 2);
    assert_eq!(
        roots
            .iter()
            .map(|root| root.trace_id)
            .collect::<HashSet<_>>()
            .len(),
        2
    );

    for root in &roots {
        let trace = records
            .iter()
            .filter(|record| record.trace_id == root.trace_id)
            .collect::<Vec<_>>();
        let names = trace
            .iter()
            .map(|record| record.name.as_ref())
            .collect::<HashSet<_>>();
        for expected in [
            "scheduler.ack",
            "crawler.execute",
            "middleware.before_download",
            "downloader.fetch",
            "middleware.after_download",
            "middleware.before_parse",
            "executor.parse",
            "output.items",
            "middleware.before_item",
            "item_store.submit",
            "scheduler.success",
        ] {
            assert!(names.contains(expected), "missing span: {expected}");
        }

        let execution = span(&trace, "crawler.execute");
        let parse = span(&trace, "executor.parse");
        let output = span(&trace, "output.items");
        assert_eq!(execution.parent_id, root.span_id);
        assert_eq!(
            span(&trace, "downloader.fetch").parent_id,
            execution.span_id
        );
        assert_eq!(parse.parent_id, execution.span_id);
        assert_eq!(output.parent_id, parse.span_id);
        assert_eq!(span(&trace, "item_store.submit").parent_id, output.span_id);
        assert_eq!(span(&trace, "scheduler.success").parent_id, root.span_id);

        assert_eq!(property(root, "span.status_code"), Some("ok"));
        let fetch = span(&trace, "downloader.fetch");
        assert_eq!(property(fetch, "http.request.method"), Some("GET"));
        assert_eq!(property(fetch, "http.response.status_code"), Some("200"));
        assert_eq!(property(fetch, "retry.attempt"), Some("1"));
        assert_eq!(
            property(span(&trace, "item_store.submit"), "retry.attempt"),
            Some("1")
        );
    }

    let retried_store = roots
        .iter()
        .map(|root| {
            records
                .iter()
                .filter(|record| {
                    record.trace_id == root.trace_id && record.name == "item_store.submit"
                })
                .collect::<Vec<_>>()
        })
        .find(|spans| spans.len() == 2)
        .expect("one Item submission should retry");
    let first = retried_store
        .iter()
        .copied()
        .find(|record| property(record, "retry.attempt") == Some("1"))
        .expect("first Store attempt");
    let second = retried_store
        .iter()
        .copied()
        .find(|record| property(record, "retry.attempt") == Some("2"))
        .expect("second Store attempt");
    assert_eq!(property(first, "span.status_code"), Some("error"));
    assert_eq!(property(first, "error.type"), Some("item_store"));
    assert_eq!(property(second, "span.status_code"), Some("ok"));

    assert!(records.iter().all(|record| {
        !record.name.contains(SECRET)
            && record
                .properties
                .iter()
                .all(|(key, value)| !key.contains(SECRET) && !value.contains(SECRET))
    }));
}

fn span<'a>(records: &'a [&SpanRecord], name: &str) -> &'a SpanRecord {
    records
        .iter()
        .copied()
        .find(|record| record.name == name)
        .unwrap_or_else(|| panic!("missing span: {name}"))
}

fn property<'a>(record: &'a SpanRecord, name: &str) -> Option<&'a str> {
    record
        .properties
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_ref())
}
