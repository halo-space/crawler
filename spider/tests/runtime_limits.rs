use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use spider::scheduler::{Init, Scheduler};
use spider::{downloader, engine, net, payload};

struct ClaimScheduler {
    inner: spider::Memory,
    calls: Mutex<Vec<(usize, usize)>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    transient: AtomicUsize,
    pending_failures: AtomicUsize,
    local: bool,
}

impl ClaimScheduler {
    fn new(transient: usize) -> Self {
        Self {
            inner: spider::Memory::new("worker-1"),
            calls: Mutex::new(Vec::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            transient: AtomicUsize::new(transient),
            pending_failures: AtomicUsize::new(0),
            local: true,
        }
    }

    fn remote() -> Self {
        Self {
            local: false,
            ..Self::new(0)
        }
    }

    fn with_pending_failures(mut self, failures: usize) -> Self {
        self.pending_failures = AtomicUsize::new(failures);
        self
    }
}

impl Scheduler for ClaimScheduler {
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
        self.calls.lock().unwrap().push((limit, requests.len()));
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(requests)
    }

    async fn has_pending_requests(&self) -> Result<bool, spider::scheduler::Error> {
        if self
            .pending_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_sub(1)
            })
            .is_ok()
        {
            return Err(spider::scheduler::Error::Unavailable(
                "temporary pending-state failure".to_string(),
            ));
        }
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
        let url = request.url.clone();
        Ok(net::Response {
            vals: request.vals.clone(),
            kwargs: request.kwargs.clone(),
            middlewares: request.middlewares.clone(),
            request,
            url,
            status: net::StatusCode(200),
            reason: Some("OK".to_string()),
            version: net::HttpVersion::Http11,
            redirects: Vec::new(),
            headers: net::Headers::new(),
            cookies: net::Cookies::new(),
            body: Bytes::new(),
        })
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

fn requests(count: usize) -> Vec<net::Request> {
    (0..count)
        .map(|index| net::Request::follow(format!("https://example.com/{index}")).unwrap())
        .collect()
}

#[tokio::test]
async fn claim_limit_fills_concurrency_in_multiple_single_claims() {
    let scheduler = ClaimScheduler::new(0);
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
        .with_limit(4);

    runtime.start().await.unwrap();

    assert_eq!(max_active.load(Ordering::SeqCst), 16);
    assert_eq!(runtime.scheduler().max_active.load(Ordering::SeqCst), 1);
    let calls = runtime.scheduler().calls.lock().unwrap();
    assert_eq!(&calls[..4], &[(4, 4), (4, 4), (4, 4), (4, 4)]);
    assert!(calls.iter().all(|(limit, _)| *limit <= 4));
}

#[tokio::test]
async fn transient_claim_failure_is_retried_without_losing_requests() {
    let scheduler = ClaimScheduler::new(1);
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
        .with_limit(1);

    runtime.start().await.unwrap();

    assert_eq!(runtime.scheduler().inner.done_len(), 1);
    assert_eq!(runtime.scheduler().inner.processing_len(), 0);
    assert_eq!(runtime.scheduler().max_active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_state_failure_drains_an_active_request_before_returning() {
    let scheduler = ClaimScheduler::new(0).with_pending_failures(3);
    scheduler
        .push(payload::Payload::new().requests(requests(1)))
        .await
        .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(SlowDownload {
            active: active.clone(),
            max_active: Arc::new(AtomicUsize::new(0)),
        })
        .with_spider(EmptySpider::new())
        .build()
        .with_concurrency(2)
        .with_limit(1);

    let error = runtime.start().await.unwrap_err();

    assert!(error.to_string().contains("pending-state failure"));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.scheduler().inner.done_len(), 1);
    assert_eq!(runtime.scheduler().inner.processing_len(), 0);
}

#[tokio::test]
async fn request_concurrency_above_the_default_is_not_clamped() {
    let concurrency = engine::MAX_REQUEST_CONCURRENCY + 1;
    let scheduler = ClaimScheduler::new(0);
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

    runtime.start().await.unwrap();

    assert_eq!(max_active.load(Ordering::SeqCst), concurrency);
}

#[tokio::test]
async fn remote_scheduler_consumes_existing_requests_without_creating_a_seed() {
    let scheduler = ClaimScheduler::remote();
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

    runtime.start().await.unwrap();

    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.scheduler().inner.trace_len(), 0);
    assert_eq!(runtime.scheduler().inner.done_len(), 1);
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
        .with_limit(0);
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
}
