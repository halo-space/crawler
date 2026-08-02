use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use spider::scheduler::{Init, Scheduler};
use spider::{downloader, engine, net, payload};

#[path = "support/ai.rs"]
mod ai;

#[derive(Debug, PartialEq, Eq)]
struct Claim {
    limit: usize,
    count: usize,
}

struct ClaimScheduler {
    inner: spider::Memory,
    calls: Mutex<Vec<Claim>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    transient: AtomicUsize,
    opened_concurrency: AtomicUsize,
    local: bool,
}

impl ClaimScheduler {
    fn new(transient: usize) -> Self {
        Self {
            inner: spider::Memory::new(),
            calls: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            transient: AtomicUsize::new(transient),
            opened_concurrency: AtomicUsize::new(0),
            local: true,
        }
    }

    fn remote() -> Self {
        Self {
            local: false,
            ..Self::new(0)
        }
    }

    fn with_modes(mut self, modes: impl IntoIterator<Item = net::Mode>) -> Self {
        self.inner = spider::Memory::new().with_modes(modes);
        self
    }
}

impl Scheduler for ClaimScheduler {
    fn lease(&self) -> Option<spider::scheduler::Lease> {
        self.inner.lease()
    }

    async fn open(&self, concurrency: usize) -> Result<(), spider::scheduler::Error> {
        self.opened_concurrency.store(concurrency, Ordering::SeqCst);
        self.inner.open(concurrency).await
    }

    async fn close(&self) -> Result<(), spider::scheduler::Error> {
        self.inner.close().await
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.push(payload).await
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
    ) -> Result<Vec<net::Request>, spider::scheduler::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        if self
            .transient
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            self.active.fetch_sub(1, Ordering::SeqCst);
            return Err(spider::scheduler::Error::Unavailable(
                "temporary claim failure".to_string(),
            ));
        }
        let requests = self.inner.next_requests(limit).await?;
        self.calls.lock().unwrap().push(Claim {
            limit,
            count: requests.len(),
        });
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(requests)
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        self.inner.has_pending_requests().await
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
        self.inner.success(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), spider::scheduler::Error> {
        self.inner.failure(payload).await
    }
}

impl Init for ClaimScheduler {
    fn initializes_run(&self) -> bool {
        self.local
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

struct SlowDownload {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl downloader::Download for SlowDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut response = net::Response::new(request, net::StatusCode(200), Bytes::new());
        response.reason = Some("OK".to_string());
        Ok(response)
    }
}

#[macros::spider]
struct EmptySpider;

#[macros::spider]
impl EmptySpider {
    fn name(&self) -> &str {
        "runtime-limits"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        Ok(())
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

#[macros::spider]
struct ConsumeSpider {
    starts: Arc<AtomicUsize>,
}

#[macros::spider]
impl ConsumeSpider {
    fn name(&self) -> &str {
        "consume-only"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }
}

const FIXTURE_TASK: &str = "runtime-limits-fixture";
const FIXTURE_TRACE: &str = "runtime-limits-trace";

fn bound_request(url: impl Into<String>) -> net::Request {
    let mut request = net::Request::follow(url).unwrap();
    request.task_id = FIXTURE_TASK.to_string();
    request.trace_id = FIXTURE_TRACE.to_string();
    request
}

fn requests(count: usize) -> Vec<net::Request> {
    (0..count)
        .map(|index| bound_request(format!("https://example.com/{index}")))
        .collect()
}

async fn init_run(scheduler: &impl Init) {
    scheduler
        .init(
            FIXTURE_TRACE.to_string(),
            spider::trace::Snapshot::code(FIXTURE_TASK),
            Vec::new(),
        )
        .await
        .unwrap();
}

const REMOTE_AI_TRACE: &str = "remote-ai-trace";

fn ai_rules() -> spider::config::Config {
    spider::config::Config::from_yaml(
        r#"
spider:
  name: remote-ai-rules
  start: [{node: index, url: https://example.com/article}]
graph:
  nodes:
    index:
      parse:
        fields:
          article:
            extractors:
              - kind: ai
                expr: extract the article as JSON
  edges: []
"#,
    )
    .unwrap()
}

async fn remote_rules(config: &spider::config::Config) -> ClaimScheduler {
    let scheduler = ClaimScheduler::remote();
    let task_id = config.spider.name.clone();
    let requests = config
        .initial_requests(task_id.clone(), REMOTE_AI_TRACE, Default::default())
        .unwrap();
    scheduler
        .init(
            REMOTE_AI_TRACE.to_string(),
            spider::trace::Snapshot::rules(task_id, config.clone()),
            requests,
        )
        .await
        .unwrap();
    scheduler
}

fn ai_downloader() -> SlowDownload {
    SlowDownload {
        active: Arc::new(AtomicUsize::new(0)),
        max_active: Arc::new(AtomicUsize::new(0)),
    }
}

#[tokio::test]
async fn claim_limit_fills_concurrency_in_multiple_single_claims() {
    let scheduler = ClaimScheduler::new(0);
    init_run(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(requests(16)))
        .await
        .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active,
            max_active: max_active.clone(),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(16)
        .with_claim_limit(4);

    runtime.start_until_idle().await.unwrap();

    assert_eq!(max_active.load(Ordering::SeqCst), 16);
    assert_eq!(runtime.scheduler().max_active.load(Ordering::SeqCst), 1);
    let calls = runtime.scheduler().calls.lock().unwrap();
    assert_eq!(
        &calls[..4],
        [
            Claim { limit: 4, count: 4 },
            Claim { limit: 4, count: 4 },
            Claim { limit: 4, count: 4 },
            Claim { limit: 4, count: 4 },
        ]
    );
    assert!(calls.iter().all(|claim| claim.limit <= 4));
}

#[tokio::test]
async fn scheduler_owns_request_modes() {
    let scheduler = ClaimScheduler::new(0).with_modes([net::Mode::Browser]);
    init_run(&scheduler).await;
    let request = bound_request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build();

    runtime.start_until_idle().await.unwrap();

    let scheduler = runtime.scheduler();
    let calls = scheduler.calls.lock().unwrap();
    assert!(calls.iter().any(|claim| claim.count == 1));
}

#[tokio::test]
async fn scheduler_open_receives_frozen_request_concurrency() {
    let mut runtime = engine::Builder::new()
        .with_scheduler(ClaimScheduler::new(0))
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(7);

    runtime.start_until_idle().await.unwrap();

    assert_eq!(
        runtime
            .scheduler()
            .opened_concurrency
            .load(Ordering::SeqCst),
        7
    );
}

#[tokio::test]
async fn rules_builder_uses_scheduler_request_modes() {
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: browser-rules
  start:
    - node: index
      url: https://example.com/browser
      download_mode: browser
graph:
  nodes:
    index: {}
  edges: []
"#,
    )
    .unwrap();
    let mut runtime = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .with_rules(config)
        .with_scheduler(ClaimScheduler::new(0).with_modes([net::Mode::Browser]))
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .build();

    runtime.start_until_idle().await.unwrap();

    let scheduler = runtime.scheduler();
    let calls = scheduler.calls.lock().unwrap();
    assert!(calls.iter().any(|claim| claim.count == 1));
}

#[tokio::test]
async fn transient_claim_failure_is_retried_without_losing_requests() {
    let scheduler = ClaimScheduler::new(1);
    init_run(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(requests(1)))
        .await
        .unwrap();
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(1)
        .with_claim_limit(1);

    runtime.start_until_idle().await.unwrap();

    assert_eq!(runtime.scheduler().inner.done_len(), 1);
    assert_eq!(runtime.scheduler().inner.processing_len(), 0);
    assert_eq!(runtime.scheduler().max_active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn request_concurrency_above_the_default_is_not_clamped() {
    let concurrency = engine::MAX_REQUEST_CONCURRENCY + 1;
    let scheduler = ClaimScheduler::new(0);
    init_run(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(requests(concurrency)))
        .await
        .unwrap();
    let max_active = Arc::new(AtomicUsize::new(0));
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active.clone(),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(concurrency);

    runtime.start_until_idle().await.unwrap();

    assert_eq!(max_active.load(Ordering::SeqCst), concurrency);
}

#[tokio::test]
async fn remote_scheduler_consumes_existing_requests_without_creating_a_seed() {
    let scheduler = ClaimScheduler::remote();
    init_run(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(requests(1)))
        .await
        .unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(ConsumeSpider::new(starts.clone()))
        .build();

    runtime.start_until_idle().await.unwrap();

    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.scheduler().inner.trace_len(), 1);
    assert_eq!(runtime.scheduler().inner.done_len(), 1);
}

#[tokio::test]
async fn remote_rules_trace_uses_worker_openai_without_persisting_it() {
    let config = ai_rules();
    let scheduler = remote_rules(&config).await;
    let provider = ai::Server::start([r#"{"title":"Remote Rust"}"#]);
    let openai = spider::ai::OpenAI::new(
        provider.base_url(),
        "provider-sentinel-secret",
        "provider-sentinel-model",
    )
    .unwrap();
    let trace = scheduler.trace(REMOTE_AI_TRACE).await.unwrap().unwrap();
    let encoded = serde_json::to_string(&trace).unwrap();
    for sentinel in [
        provider.base_url(),
        "provider-sentinel-secret",
        "provider-sentinel-model",
    ] {
        assert!(!encoded.contains(sentinel), "{sentinel}");
    }
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_ai(openai)
        .with_downloader(ai_downloader())
        .with_spider(EmptySpider::new())
        .build();

    runtime.start_until_idle().await.unwrap();

    assert_eq!(runtime.scheduler().inner.done_len(), 1);
    assert_eq!(runtime.scheduler().inner.failed_len(), 0);
    let requests = provider.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v1/chat/completions");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer provider-sentinel-secret")
    );
    assert_eq!(
        requests[0].body["model"],
        serde_json::Value::from("provider-sentinel-model")
    );
}

#[tokio::test]
async fn remote_rules_trace_without_worker_openai_records_the_failure() {
    let scheduler = remote_rules(&ai_rules()).await;
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(ai_downloader())
        .with_spider(EmptySpider::new())
        .build();

    runtime.start_until_idle().await.unwrap();

    assert_eq!(runtime.scheduler().inner.done_len(), 0);
    assert_eq!(runtime.scheduler().inner.failed_len(), 1);
    assert_eq!(runtime.scheduler().inner.processing_len(), 0);
    assert!(
        runtime
            .scheduler()
            .inner
            .errors()
            .iter()
            .any(|error| error.contains("AI provider is not configured"))
    );
}

#[tokio::test]
async fn zero_runtime_limits_are_rejected_before_startup() {
    let mut concurrency = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(0);
    assert!(
        concurrency
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("concurrency must be positive")
    );

    let mut claim = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .build()
        .with_claim_limit(0);
    assert!(
        claim
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("claim limit must be positive")
    );

    let mut events = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .build()
        .with_event_limit(0);
    assert!(
        events
            .start()
            .await
            .unwrap_err()
            .to_string()
            .contains("Event limit must be positive")
    );

    let mut idle = engine::Builder::new()
        .with_spider(EmptySpider::new())
        .build()
        .with_idle_interval(std::time::Duration::ZERO);
    assert!(
        idle.start()
            .await
            .unwrap_err()
            .to_string()
            .contains("Idle interval must be positive")
    );
}
