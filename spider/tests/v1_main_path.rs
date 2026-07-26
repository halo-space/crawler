use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde_json::Value;
use spider::item::Item;
use spider::middleware::{BoxFuture, Middleware, Spec};
use spider::scheduler::{Init, Scheduler};
use spider::{downloader, engine, net, payload};

static TEMP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct PayloadRecord {
    id: String,
    state: payload::State,
    stats: HashMap<String, Value>,
    error: Option<String>,
}

struct RecordingScheduler {
    inner: spider::Memory,
    records: Arc<Mutex<Vec<PayloadRecord>>>,
    item_attempts: Option<Arc<AtomicUsize>>,
    item_failures: usize,
    block_snapshots: bool,
}

impl RecordingScheduler {
    fn new(records: Arc<Mutex<Vec<PayloadRecord>>>) -> Self {
        Self {
            inner: spider::Memory::new(),
            records,
            item_attempts: None,
            item_failures: 0,
            block_snapshots: false,
        }
    }

    fn with_items(
        mut self,
        dir: impl Into<PathBuf>,
        attempts: Arc<AtomicUsize>,
        failures: usize,
    ) -> Self {
        self.inner = self.inner.with_dir(dir);
        self.item_attempts = Some(attempts);
        self.item_failures = failures;
        self
    }

    fn block_snapshots(mut self) -> Self {
        self.block_snapshots = true;
        self
    }

    fn trace_stats(&self) -> HashMap<String, spider::stats::Counter> {
        let trace_ids = self.inner.trace_ids();
        assert_eq!(trace_ids.len(), 1);
        self.inner.trace_stats(&trace_ids[0])
    }
}

impl Scheduler for RecordingScheduler {
    fn dir(&self) -> Option<&Path> {
        self.inner.dir()
    }

    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.open().await
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        if let Some(attempts) = &self.item_attempts {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= self.item_failures {
                if self.block_snapshots
                    && let Some(dir) = self.dir()
                {
                    let snapshots = dir.join("data").join("items").join("snapshots");
                    let _ = tokio::fs::remove_dir_all(&snapshots).await;
                    tokio::fs::write(&snapshots, b"blocked")
                        .await
                        .map_err(|error| spider::scheduler::Error::Message(error.to_string()))?;
                }
                return Err(spider::scheduler::Error::Message("item submit".to_string()));
            }
        }
        self.inner.push_items(payload).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, spider::scheduler::Error> {
        self.inner.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        self.inner.next_requests(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, spider::scheduler::Error> {
        self.inner.has_pending_requests(worker_id, modes).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.release(payload).await
    }

    async fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.records.lock().unwrap().push(PayloadRecord {
            id: payload.id.clone(),
            state: payload.state,
            stats: payload.stats.clone(),
            error: payload.error.clone(),
        });
        self.inner.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.records.lock().unwrap().push(PayloadRecord {
            id: payload.id.clone(),
            state: payload.state,
            stats: payload.stats.clone(),
            error: payload.error.clone(),
        });
        self.inner.failure(payload).await
    }
}

impl Init for RecordingScheduler {
    fn initializes_run(&self) -> bool {
        self.inner.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: spider::trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), spider::scheduler::Error> {
        self.inner.init(trace_id, snapshot, requests).await
    }
}

struct StatusDownload {
    status: u16,
}

impl downloader::Download for StatusDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        Ok(response(request, self.status))
    }
}

struct FlakyDownload {
    attempts: Arc<AtomicUsize>,
    failures: usize,
}

impl downloader::Download for FlakyDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= self.failures {
            return Err(downloader::Error::UnsupportedMode("temporary".to_string()));
        }
        Ok(response(request, 200))
    }
}

fn response(request: net::Request, status: u16) -> net::Response {
    net::Response::new(request, net::StatusCode(status), Bytes::new())
}

#[derive(serde::Serialize)]
struct TestItem {
    #[serde(skip)]
    state: spider::item::State,
    title: Option<String>,
    #[serde(skip)]
    middlewares: Vec<Spec>,
}

impl TestItem {
    fn valid() -> Self {
        Self {
            state: spider::item::State::default(),
            title: Some("book".to_string()),
            middlewares: Vec::new(),
        }
    }

    fn with_middlewares(mut self, middlewares: Vec<Spec>) -> Self {
        self.middlewares = middlewares;
        self
    }
}

impl Item for TestItem {
    fn from_values(mut values: spider::item::Values) -> Result<Self, spider::item::Error> {
        let title = values
            .shift_remove("title")
            .and_then(|value| value.as_str().map(str::to_string));
        Ok(Self {
            state: spider::item::State::default(),
            title,
            middlewares: Vec::new(),
        })
    }

    fn state(&self) -> &spider::item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut spider::item::State {
        &mut self.state
    }

    fn middlewares(&self) -> &[Spec] {
        &self.middlewares
    }
}

#[macros::spider]
struct ItemSpider;

#[macros::spider]
impl ItemSpider {
    fn name(&self) -> &str {
        "item"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.tx.item(vec![TestItem::valid()]).await
    }
}

#[macros::spider]
struct NoopSpider {
    calls: Arc<AtomicUsize>,
}

#[macros::spider]
impl NoopSpider {
    fn name(&self) -> &str {
        "noop"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[macros::spider]
struct DedupSpider;

#[macros::spider]
impl DedupSpider {
    fn name(&self) -> &str {
        "dedup"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let request = net::Request::follow("https://example.com/list")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        self.tx.request(vec![request]).await
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        let first = response
            .follow("/detail")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .node(Self::detail)
            .with_dedup(["$request.url"], Some(60_000));
        let second = response
            .follow("/detail")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .node(Self::detail)
            .with_dedup(["$request.url"], Some(60_000));
        self.tx.request(vec![first, second]).await
    }

    async fn detail(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct ValidateItemSpider;

#[macros::spider]
impl ValidateItemSpider {
    fn name(&self) -> &str {
        "validate_item"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        let item = TestItem {
            state: spider::item::State::default(),
            title: None,
            middlewares: vec![
                Spec::new("validate")
                    .hook("before_item")
                    .args(serde_json::json!({"required": ["title"]})),
            ],
        };
        self.tx.item(vec![item]).await
    }
}

#[macros::spider]
struct ValidateRequestSpider;

#[macros::spider]
impl ValidateRequestSpider {
    fn name(&self) -> &str {
        "validate_request"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        let mut request = net::Request::follow("https://example.com/invalid")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        request.url = "not-an-absolute-url".to_string();
        self.tx.request(vec![request]).await
    }
}

#[macros::spider]
struct ParseRetrySpider {
    attempts: Arc<AtomicUsize>,
}

#[macros::spider]
impl ParseRetrySpider {
    fn name(&self) -> &str {
        "parse_retry"
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let request = response
            .follow("/detail")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .node(Self::detail);
        self.tx.request(vec![request]).await?;
        if attempt < 3 {
            return Err(spider::Error::Message("temporary parse".to_string()));
        }
        Ok(())
    }

    async fn detail(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct QueueRetrySpider {
    attempts: Arc<AtomicUsize>,
}

#[macros::spider]
impl QueueRetrySpider {
    fn name(&self) -> &str {
        "queue_retry"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        let mut request = net::Request::follow("https://example.com")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        request.max_retry_count = 2;
        self.tx.request(vec![request]).await
    }

    async fn index(&self, response: net::Response) -> Result<(), spider::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let request = response
            .follow("/detail")
            .map_err(|error| spider::Error::Message(error.to_string()))?
            .node(Self::detail);
        self.tx.request(vec![request]).await?;
        Err(spider::Error::Message("parse failed".to_string()))
    }

    async fn detail(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct RetryItemSpider;

#[macros::spider]
impl RetryItemSpider {
    fn name(&self) -> &str {
        "retry_item"
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        let retry = Spec::new("retry")
            .hook("error_item")
            .args(serde_json::json!({"count": 2, "backoff": [0, 0]}));
        self.tx
            .item(vec![TestItem::valid().with_middlewares(vec![retry])])
            .await
    }
}

struct SpiderLifecycle {
    calls: Arc<Mutex<Vec<&'static str>>>,
    fail_before: bool,
}

impl Middleware for SpiderLifecycle {
    fn before_spider<'a>(&'a self, _spec: &'a Spec) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("before_spider");
            if self.fail_before {
                return Err(spider::middleware::Error::Message(
                    "before spider".to_string(),
                ));
            }
            Ok(())
        })
    }

    fn after_spider<'a>(&'a self, _spec: &'a Spec) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("after_spider");
            Ok(())
        })
    }
}

#[macros::spider]
struct LifecycleSpider {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

#[macros::spider]
struct FailingStartSpider;

#[macros::spider]
impl FailingStartSpider {
    fn name(&self) -> &str {
        "failing_start"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        Err(spider::Error::Message("start failed".to_string()))
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
impl LifecycleSpider {
    fn name(&self) -> &str {
        "lifecycle"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        self.calls.lock().unwrap().push("start");
        let request = net::Request::follow("https://example.com")
            .map_err(|error| spider::Error::Message(error.to_string()))?;
        self.tx.request(vec![request]).await
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.calls.lock().unwrap().push("index");
        Ok(())
    }
}

fn counter(record: &PayloadRecord, name: &str) -> spider::stats::Counter {
    serde_json::from_value(record.stats[name].clone()).unwrap()
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!(
        "spider-v1-main-path-{}-{}",
        std::process::id(),
        TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn snapshot_files(runtime_dir: &Path, task_id: &str) -> Vec<PathBuf> {
    let task_dir = runtime_dir
        .join("data")
        .join("items")
        .join("snapshots")
        .join(task_id);
    let Ok(mut hours) = tokio::fs::read_dir(task_dir).await else {
        return Vec::new();
    };
    let mut files = Vec::new();
    while let Some(hour) = hours.next_entry().await.unwrap() {
        let mut directory = tokio::fs::read_dir(hour.path()).await.unwrap();
        while let Some(entry) = directory.next_entry().await.unwrap() {
            if entry
                .path()
                .extension()
                .is_some_and(|value| value == "json")
            {
                files.push(entry.path());
            }
        }
    }
    files
}

#[tokio::test]
async fn payload_contains_request_and_item_stats() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.id = "stats-success".to_string();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(ItemSpider::new())
        .build();

    engine.start().await.unwrap();

    let records = records.lock().unwrap();
    let record = records
        .iter()
        .find(|record| record.id == "stats-success")
        .unwrap();
    assert_eq!(record.state, payload::State::Done);
    assert_eq!(counter(record, "index").total, 1);
    assert_eq!(counter(record, "index").done, 1);
    assert_eq!(counter(record, "items").total, 1);
    assert_eq!(counter(record, "items").done, 1);
}

#[tokio::test]
async fn default_validate_filters_response_before_parse_and_records_stats() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    scheduler
        .push(payload::Payload::new().requests(vec![
            net::Request::follow("https://example.com/not-found").unwrap(),
        ]))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 404 })
        .with_spider(NoopSpider::new(calls.clone()))
        .build();

    engine.start().await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let records = records.lock().unwrap();
    let stats = counter(&records[0], "index");
    assert_eq!(stats.total, 1);
    assert_eq!(stats.done, 1);
    assert_eq!(stats.filter, 1);
}

#[tokio::test]
async fn download_retry_recovers_without_recording_final_download_failure() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    let attempts = Arc::new(AtomicUsize::new(0));
    let request = net::Request::follow("https://example.com")
        .unwrap()
        .with_retry(2, [0, 0]);
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(FlakyDownload {
            attempts: attempts.clone(),
            failures: 2,
        })
        .with_spider(NoopSpider::new(Arc::new(AtomicUsize::new(0))))
        .build();

    engine.start().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    let records = records.lock().unwrap();
    let stats = counter(&records[0], "index");
    assert_eq!(stats.done, 1);
    assert_eq!(stats.download, 0);
}

#[tokio::test]
async fn final_download_failure_records_download_stats() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    scheduler
        .push(
            payload::Payload::new()
                .requests(vec![net::Request::follow("https://example.com").unwrap()]),
        )
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(FlakyDownload {
            attempts: Arc::new(AtomicUsize::new(0)),
            failures: usize::MAX,
        })
        .with_spider(NoopSpider::new(Arc::new(AtomicUsize::new(0))))
        .build();

    engine.start().await.unwrap();

    let records = records.lock().unwrap();
    assert_eq!(records[0].state, payload::State::Failed);
    let stats = counter(&records[0], "index");
    assert_eq!(stats.total, 1);
    assert_eq!(stats.done, 0);
    assert_eq!(stats.download, 1);
}

#[tokio::test]
async fn dedup_skips_duplicate_request_and_memory_accumulates_stats() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records);
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(DedupSpider::new())
        .build();

    engine.start().await.unwrap();

    let stats = engine.scheduler().trace_stats();
    assert_eq!(stats["index"].total, 1);
    assert_eq!(stats["index"].done, 1);
    assert_eq!(stats["detail"].total, 2);
    assert_eq!(stats["detail"].done, 1);
    assert_eq!(stats["detail"].dedup, 1);
}

#[tokio::test]
async fn item_validate_skip_does_not_fail_current_request() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    scheduler
        .push(
            payload::Payload::new()
                .requests(vec![net::Request::follow("https://example.com").unwrap()]),
        )
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(ValidateItemSpider::new())
        .build();

    engine.start().await.unwrap();

    let records = records.lock().unwrap();
    assert_eq!(records[0].state, payload::State::Done);
    let stats = counter(&records[0], "items");
    assert_eq!(stats.total, 1);
    assert_eq!(stats.done, 0);
    assert_eq!(stats.validate, 1);
}

#[tokio::test]
async fn request_validate_skip_does_not_fail_current_request() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    scheduler
        .push(
            payload::Payload::new()
                .requests(vec![net::Request::follow("https://example.com").unwrap()]),
        )
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(ValidateRequestSpider::new())
        .build();

    engine.start().await.unwrap();

    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, payload::State::Done);
    let stats = counter(&records[0], "index");
    assert_eq!(stats.total, 2);
    assert_eq!(stats.done, 1);
    assert_eq!(stats.validate, 1);
}

#[tokio::test]
async fn parse_retry_reinvokes_handler_until_it_succeeds() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(records.clone());
    let attempts = Arc::new(AtomicUsize::new(0));
    let request = net::Request::follow("https://example.com")
        .unwrap()
        .with_retry(2, [0, 0]);
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(ParseRetrySpider::new(attempts.clone()))
        .build();

    engine.start().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(engine.scheduler().inner.done_len(), 2);
    assert!(
        records
            .lock()
            .unwrap()
            .iter()
            .all(|record| record.state == payload::State::Done)
    );
}

#[tokio::test]
async fn queue_retry_replays_the_same_child_request() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_scheduler(RecordingScheduler::new(records))
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(QueueRetrySpider::new(attempts.clone()))
        .build();

    engine.start().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(engine.scheduler().inner.done_len(), 1);
    assert_eq!(engine.scheduler().inner.failed_len(), 1);
}

#[tokio::test]
async fn item_retry_removes_failure_snapshot_after_recovery() {
    let runtime_dir = temp_dir();
    let records = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let scheduler =
        RecordingScheduler::new(records.clone()).with_items(&runtime_dir, attempts.clone(), 1);
    scheduler
        .inner
        .init(
            "trace-item".to_string(),
            spider::trace::Snapshot::code("task"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task".to_string();
    request.trace_id = "trace-item".to_string();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(RetryItemSpider::new())
        .build();

    engine.start().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(snapshot_files(&runtime_dir, "task").await.is_empty());
    assert_eq!(records.lock().unwrap()[0].state, payload::State::Done);
    tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
}

#[tokio::test]
async fn final_item_failure_keeps_complete_local_snapshot() {
    let runtime_dir = temp_dir();
    let records = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let scheduler =
        RecordingScheduler::new(records.clone()).with_items(&runtime_dir, attempts.clone(), 3);
    scheduler
        .inner
        .init(
            "trace-item".to_string(),
            spider::trace::Snapshot::code("task"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.id = "request-1".to_string();
    request.task_id = "task".to_string();
    request.trace_id = "trace-item".to_string();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(RetryItemSpider::new())
        .build();

    engine.start().await.unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(records.lock().unwrap()[0].state, payload::State::Failed);
    let files = snapshot_files(&runtime_dir, "task").await;
    assert_eq!(files.len(), 1);
    let snapshot: Value =
        serde_json::from_slice(&tokio::fs::read(&files[0]).await.unwrap()).unwrap();
    assert_eq!(snapshot["id"], "request-1");
    let item_id = snapshot["items"][0]["id"].as_str().unwrap();
    assert_eq!(
        uuid::Uuid::parse_str(item_id).unwrap().get_version(),
        Some(uuid::Version::SortRand)
    );
    assert_eq!(snapshot["items"][0]["data"]["title"], "book");
    assert!(snapshot["error"].as_str().unwrap().contains("item submit"));
    tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
}

#[tokio::test]
async fn snapshot_write_failure_preserves_original_submit_error() {
    let runtime_dir = temp_dir();
    let records = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let scheduler = RecordingScheduler::new(records.clone())
        .with_items(&runtime_dir, attempts, 1)
        .block_snapshots();
    scheduler
        .inner
        .init(
            "trace-item".to_string(),
            spider::trace::Snapshot::code("task"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task".to_string();
    request.trace_id = "trace-item".to_string();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(ItemSpider::new())
        .build();

    engine.start().await.unwrap();

    {
        let records = records.lock().unwrap();
        assert_eq!(records[0].state, payload::State::Failed);
        let error = records[0].error.as_deref().unwrap();
        assert!(error.contains("item submit"));
        assert!(error.contains("failure snapshot also failed"));
    }
    tokio::fs::remove_dir_all(runtime_dir).await.unwrap();
}

#[tokio::test]
async fn spider_lifecycle_wraps_start_and_request_processing() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(Arc::new(Mutex::new(Vec::new())));
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(LifecycleSpider::new(calls.clone()))
        .with_middleware(
            "lifecycle",
            SpiderLifecycle {
                calls: calls.clone(),
                fail_before: false,
            },
        )
        .with_spider_middleware(Spec::new("lifecycle"))
        .build();

    engine.start().await.unwrap();

    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["before_spider", "start", "index", "after_spider"]
    );
}

#[tokio::test]
async fn after_spider_runs_when_before_spider_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let scheduler = RecordingScheduler::new(Arc::new(Mutex::new(Vec::new())));
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(StatusDownload { status: 200 })
        .with_spider(LifecycleSpider::new(calls.clone()))
        .with_middleware(
            "lifecycle",
            SpiderLifecycle {
                calls: calls.clone(),
                fail_before: true,
            },
        )
        .with_spider_middleware(Spec::new("lifecycle"))
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(matches!(error, spider::Error::Middleware(_)));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["before_spider", "after_spider"]
    );
}

#[tokio::test]
async fn after_spider_runs_when_spider_start_fails() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut engine = engine::Engine::new()
        .with_spider(FailingStartSpider::new())
        .with_middleware(
            "lifecycle",
            SpiderLifecycle {
                calls: calls.clone(),
                fail_before: false,
            },
        )
        .with_spider_middleware(Spec::new("lifecycle"))
        .build();

    let error = engine.start().await.unwrap_err();

    assert!(error.to_string().contains("start failed"));
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        ["before_spider", "after_spider"]
    );
}
