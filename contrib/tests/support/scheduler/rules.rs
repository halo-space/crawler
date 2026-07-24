use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use spider::scheduler::Init;
use spider::{
    Scheduler, Spider, SpiderFactory, Tx, downloader, engine, net, payload, scheduler, trace,
};

/// A Rules trace is observable only when the Scheduler restores its Trace Snapshot onto a claim.
/// Without that attachment, Engine treats the Request as code mode and never evaluates the edge.
pub(super) async fn claimed_requests_preserve_rules_trace<S>(scheduler: S)
where
    S: Scheduler + scheduler::Init + 'static,
{
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: conformance-rules
  start: [{node: index, url: https://example.com/rules/start}]
graph:
  nodes:
    index: {}
    detail: {}
  edges:
    - from: index
      kind: request
      request:
        node: detail
        url: https://example.com/rules/detail
"#,
    )
    .unwrap();
    let fetched = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_rules(config)
        .with_spider(RulesSpiderFactory)
        .with_downloader(RulesDownload {
            fetched: fetched.clone(),
        })
        .build()
        .with_concurrency(1);
    let dir = engine.scheduler().dir().map(PathBuf::from);

    engine.start().await.unwrap();

    assert_eq!(fetched.load(Ordering::SeqCst), 2);
    if let Some(dir) = dir {
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
}

pub(super) async fn existing_rules_trace_runs_without_local_seed<S>(scheduler: S)
where
    S: Scheduler + scheduler::Init + 'static,
{
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: remote-rules
  start: [{node: detail, url: https://example.com/remote/detail}]
graph:
  nodes:
    detail: {}
  edges: []
"#,
    )
    .unwrap();
    let task_id = "remote-task";
    let trace_id = "remote-trace";
    let scheduler = NoSeed(scheduler);
    scheduler.open().await.unwrap();
    scheduler
        .init(
            trace_id.to_string(),
            trace::Snapshot::rules(task_id, config.clone()),
            config
                .initial_requests(task_id, trace_id, Default::default())
                .unwrap(),
        )
        .await
        .unwrap();
    scheduler.close().await.unwrap();

    let fetched = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let indexed = Arc::new(AtomicUsize::new(0));
    let mut engine = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_spider(RemoteSpiderFactory {
            started: started.clone(),
            indexed: indexed.clone(),
        })
        .with_downloader(RulesDownload {
            fetched: fetched.clone(),
        })
        .build()
        .with_concurrency(1);
    let dir = engine.scheduler().dir().map(PathBuf::from);

    engine.start().await.unwrap();

    assert_eq!(started.load(Ordering::SeqCst), 0);
    assert_eq!(fetched.load(Ordering::SeqCst), 1);
    assert_eq!(indexed.load(Ordering::SeqCst), 1);
    if let Some(dir) = dir {
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
}

struct RulesSpider {
    tx: Tx,
}

struct RulesSpiderFactory;

impl SpiderFactory for RulesSpiderFactory {
    type Spider = RulesSpider;

    fn build(self, tx: Tx) -> Self::Spider {
        RulesSpider { tx }
    }
}

impl Spider for RulesSpider {
    type Item = spider::item::Map;

    fn name(&self) -> &str {
        "conformance-rules"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

struct RulesDownload {
    fetched: Arc<AtomicUsize>,
}

struct NoSeed<S>(S);

impl<S> Scheduler for NoSeed<S>
where
    S: Scheduler,
{
    fn dir(&self) -> Option<&Path> {
        self.0.dir()
    }

    fn lease(&self) -> Option<scheduler::Lease> {
        self.0.lease()
    }

    async fn open(&self) -> Result<(), scheduler::Error> {
        self.0.open().await
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        self.0.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        self.0.push(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.push_items(payload).await
    }

    async fn trace(&self, trace_id: &str) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        self.0.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        self.0.next_requests(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        self.0.has_pending_requests(worker_id, modes).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.ack(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.release(payload).await
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.refresh_lease(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.0.failure(payload).await
    }
}

impl<S> scheduler::Init for NoSeed<S>
where
    S: scheduler::Init,
{
    fn initializes_run(&self) -> bool {
        false
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        self.0.init(trace_id, snapshot, requests).await
    }
}

struct RemoteSpider {
    tx: Tx,
    started: Arc<AtomicUsize>,
    indexed: Arc<AtomicUsize>,
}

struct RemoteSpiderFactory {
    started: Arc<AtomicUsize>,
    indexed: Arc<AtomicUsize>,
}

impl SpiderFactory for RemoteSpiderFactory {
    type Spider = RemoteSpider;

    fn build(self, tx: Tx) -> Self::Spider {
        RemoteSpider {
            tx,
            started: self.started,
            indexed: self.indexed,
        }
    }
}

impl Spider for RemoteSpider {
    type Item = spider::item::Map;

    fn name(&self) -> &str {
        "remote-rules"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn start(&self) -> Result<(), spider::Error> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.indexed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl downloader::Download for RulesDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        self.fetched.fetch_add(1, Ordering::SeqCst);
        let mut response = net::Response::new(request, net::StatusCode(200), Vec::new());
        response.reason = Some("OK".to_string());
        Ok(response)
    }
}
