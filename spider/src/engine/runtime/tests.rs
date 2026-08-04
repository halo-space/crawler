use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, oneshot};
use tracing::instrument::WithSubscriber;

use super::*;

struct TestDownload {
    fetches: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl downloader::Download for TestDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn fetch(
        &self,
        request: crate::net::Request,
    ) -> Result<crate::net::Response, downloader::Error> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Ok(crate::net::Response::new(
            request,
            crate::net::StatusCode(200),
            bytes::Bytes::new(),
        ))
    }
}

struct TestExecutor;

impl engine::contract::Execute for TestExecutor {
    async fn allowed_domains(
        &self,
        _request: &crate::net::Request,
    ) -> Result<Vec<String>, crate::Error> {
        Ok(Vec::new())
    }

    fn validate(&self, _request: &crate::net::Request) -> Result<(), crate::Error> {
        Ok(())
    }

    async fn parse(
        &self,
        _request: crate::net::Request,
        _response: crate::net::Response,
    ) -> Result<(), crate::Error> {
        Ok(())
    }
}

struct TestStore;

impl crate::item::Store for TestStore {
    async fn open(&self) -> Result<(), crate::item::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), crate::item::Error> {
        Ok(())
    }

    async fn submit(&self, _payload: &crate::payload::Payload) -> Result<(), crate::item::Error> {
        Ok(())
    }
}

#[derive(Clone)]
struct RecordedEvents {
    values: Arc<Mutex<Vec<String>>>,
    next_span: Arc<AtomicU64>,
}

impl RecordedEvents {
    fn new(values: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            values,
            next_span: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl tracing::Subscriber for RecordedEvents {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);

        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(&mut self.0, " {}={value:?}", field.name());
            }
        }

        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.values.lock().unwrap().push(visitor.0);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

struct IdleScheduler {
    claims: Mutex<Vec<Instant>>,
    pending_checks: AtomicUsize,
}

impl IdleScheduler {
    fn new() -> Self {
        Self {
            claims: Mutex::new(Vec::new()),
            pending_checks: AtomicUsize::new(0),
        }
    }
}

impl scheduler::Scheduler for IdleScheduler {
    async fn open(&self, _concurrency: usize) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn push(&self, _payload: crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn trace(
        &self,
        _trace_id: &str,
    ) -> Result<Option<crate::trace::Snapshot>, scheduler::Error> {
        Ok(None)
    }

    async fn next_requests(
        &self,
        _limit: usize,
    ) -> Result<Vec<crate::net::Request>, scheduler::Error> {
        self.claims.lock().unwrap().push(Instant::now());
        Ok(Vec::new())
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        Ok(self.pending_checks.fetch_add(1, Ordering::SeqCst) == 0)
    }

    async fn ack(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn release(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn refresh_lease(
        &self,
        _payload: &crate::payload::Payload,
    ) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn success(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn failure(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }
}

struct FailingDownload;

impl downloader::Download for FailingDownload {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Err(downloader::Error::InvalidConfig(
            "downloader close failed".to_string(),
        ))
    }

    async fn fetch(
        &self,
        _request: crate::net::Request,
    ) -> Result<crate::net::Response, downloader::Error> {
        unreachable!()
    }
}

struct FailingStore;

impl crate::item::Store for FailingStore {
    async fn open(&self) -> Result<(), crate::item::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), crate::item::Error> {
        Err(crate::item::Error::Message(
            "store close failed".to_string(),
        ))
    }

    async fn submit(&self, _payload: &crate::payload::Payload) -> Result<(), crate::item::Error> {
        Ok(())
    }
}

struct FailingScheduler;

impl scheduler::Scheduler for FailingScheduler {
    async fn open(&self, _concurrency: usize) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        Err(scheduler::Error::Message(
            "scheduler close failed".to_string(),
        ))
    }

    async fn push(&self, _payload: crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn trace(
        &self,
        _trace_id: &str,
    ) -> Result<Option<crate::trace::Snapshot>, scheduler::Error> {
        Ok(None)
    }

    async fn next_requests(
        &self,
        _limit: usize,
    ) -> Result<Vec<crate::net::Request>, scheduler::Error> {
        Ok(Vec::new())
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        Ok(false)
    }

    async fn ack(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn release(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn refresh_lease(
        &self,
        _payload: &crate::payload::Payload,
    ) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn success(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn failure(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }
}

struct FailingLifecycle;

impl middleware::Middleware for FailingLifecycle {
    fn before_spider<'a>(&'a self, _spec: &'a middleware::Spec) -> middleware::BoxFuture<'a, ()> {
        Box::pin(async {
            Err(middleware::Error::Message(
                "before_spider failed".to_string(),
            ))
        })
    }

    fn after_spider<'a>(&'a self, _spec: &'a middleware::Spec) -> middleware::BoxFuture<'a, ()> {
        Box::pin(async {
            Err(middleware::Error::Message(
                "after_spider failed".to_string(),
            ))
        })
    }
}

struct BlockingInit {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct ClaimScheduler {
    started: Arc<Notify>,
    release: Arc<Notify>,
    requests: std::sync::Mutex<Option<Vec<crate::net::Request>>>,
    claims: Arc<AtomicUsize>,
    successes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}

impl scheduler::Scheduler for ClaimScheduler {
    async fn open(&self, _concurrency: usize) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn push(&self, _payload: crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn trace(
        &self,
        _trace_id: &str,
    ) -> Result<Option<crate::trace::Snapshot>, scheduler::Error> {
        Ok(None)
    }

    async fn next_requests(
        &self,
        _limit: usize,
    ) -> Result<Vec<crate::net::Request>, scheduler::Error> {
        self.claims.fetch_add(1, Ordering::SeqCst);
        let requests = self
            .requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(requests) = requests else {
            return Ok(Vec::new());
        };
        self.started.notify_one();
        self.release.notified().await;
        Ok(requests)
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        Ok(false)
    }

    async fn ack(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn release(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn refresh_lease(
        &self,
        _payload: &crate::payload::Payload,
    ) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn success(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        self.successes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn failure(&self, _payload: &crate::payload::Payload) -> Result<(), scheduler::Error> {
        Ok(())
    }
}

impl engine::init::Init<scheduler::Memory> for BlockingInit {
    async fn init(
        &self,
        scheduler: Arc<scheduler::Memory>,
    ) -> Result<engine::init::Output, crate::Error> {
        let mut request = crate::net::Request::follow("https://example.com/pending")
            .map_err(|error| crate::Error::message(error.to_string()))?;
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        crate::scheduler::Init::init(
            scheduler.as_ref(),
            "trace-1".to_string(),
            crate::trace::Snapshot::code("task-1"),
            vec![request],
        )
        .await
        .map_err(crate::Error::Scheduler)?;
        self.started.notify_one();
        self.release.notified().await;
        Ok(engine::init::Output::Consume)
    }
}

#[tokio::test]
async fn signal_is_listened_for_before_open_starts() {
    let listening = Arc::new(AtomicBool::new(false));
    let signal_listening = Arc::clone(&listening);
    let (_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        signal_listening.store(true, Ordering::SeqCst);
        received
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    });

    let shutdown_requested = open_while_listening(
        async move {
            assert!(listening.load(Ordering::SeqCst));
            Ok(())
        },
        &mut shutdown,
    )
    .await
    .unwrap();

    assert_eq!(shutdown_requested, Some(false));
}

#[tokio::test]
async fn startup_signal_waits_for_open_to_finish() {
    let listening = Arc::new(AtomicBool::new(false));
    let signal_listening = Arc::clone(&listening);
    let (send_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        signal_listening.store(true, Ordering::SeqCst);
        received
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    });
    let open_started = Arc::new(Notify::new());
    let open_completed = Arc::new(Notify::new());
    let opened = Arc::new(AtomicBool::new(false));

    let started = Arc::clone(&open_started);
    let completed = Arc::clone(&open_completed);
    let did_open = Arc::clone(&opened);
    let task = tokio::spawn(async move {
        let opening = async move {
            assert!(listening.load(Ordering::SeqCst));
            started.notify_one();
            completed.notified().await;
            did_open.store(true, Ordering::SeqCst);
            Ok(())
        };
        open_while_listening(opening, &mut shutdown).await
    });

    open_started.notified().await;
    send_signal.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!opened.load(Ordering::SeqCst));
    assert!(!task.is_finished());

    open_completed.notify_one();
    assert_eq!(task.await.unwrap().unwrap(), Some(true));
    assert!(opened.load(Ordering::SeqCst));
}

#[tokio::test]
async fn ready_signal_does_not_start_open() {
    let polled = Arc::new(AtomicBool::new(false));
    let opening_polled = Arc::clone(&polled);
    let mut shutdown: ShutdownSignal = Box::pin(async { Ok(()) });

    let result = open_while_listening(
        async move {
            opening_polled.store(true, Ordering::SeqCst);
            Ok(())
        },
        &mut shutdown,
    )
    .await
    .unwrap();

    assert_eq!(result, None);
    assert!(!polled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn ready_signal_skips_an_unstarted_startup_stage() {
    let polled = Arc::new(AtomicBool::new(false));
    let stage_polled = Arc::clone(&polled);
    let mut shutdown: ShutdownSignal = Box::pin(async { Ok(()) });

    let (result, shutdown_requested) = complete_while_listening(
        async move {
            stage_polled.store(true, Ordering::SeqCst);
            Ok(())
        },
        &mut shutdown,
    )
    .await
    .unwrap();

    assert!(shutdown_requested);
    assert!(result.is_none());
    assert!(!polled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn startup_signal_waits_for_an_active_stage_then_stops_the_next_stage() {
    let (send_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        received
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    });
    let started = Arc::new(Notify::new());
    let completed = Arc::new(Notify::new());
    let stage_started = Arc::clone(&started);
    let stage_completed = Arc::clone(&completed);

    let task = tokio::spawn(async move {
        complete_while_listening(
            async move {
                stage_started.notify_one();
                stage_completed.notified().await;
                Ok("complete")
            },
            &mut shutdown,
        )
        .await
    });

    started.notified().await;
    send_signal.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    completed.notify_one();
    let (result, shutdown_requested) = task.await.unwrap().unwrap();
    assert!(shutdown_requested);
    assert_eq!(result, Some("complete"));
}

#[tokio::test]
async fn shutdown_error_waits_for_an_active_stage_before_returning() {
    let (send_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        received
            .await
            .map_err(|error| crate::Error::message(format!("shutdown signal failed: {error}")))?;
        Err(crate::Error::message("shutdown listener failed"))
    });
    let started = Arc::new(Notify::new());
    let completed = Arc::new(Notify::new());
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let stage_started = Arc::clone(&started);
    let stage_completed = Arc::clone(&completed);
    let stage_events = Arc::clone(&recorded);

    let task = tokio::spawn(async move {
        complete_while_listening(
            async move {
                stage_started.notify_one();
                stage_completed.notified().await;
                Err::<(), _>(crate::Error::message("lifecycle stage failed"))
            },
            &mut shutdown,
        )
        .with_subscriber(RecordedEvents::new(stage_events))
        .await
    });

    started.notified().await;
    send_signal.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    completed.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("shutdown error did not wait for the active stage")
        .unwrap();
    assert!(matches!(
        result,
        Err(crate::Error::Message(message)) if message == "shutdown listener failed"
    ));
    assert!(recorded.lock().unwrap().iter().any(|event| {
        event.contains("Engine lifecycle stage failed while handling a shutdown listener error")
            && event.contains("lifecycle stage failed")
    }));
}

#[tokio::test]
async fn signal_during_init_preserves_the_seed_without_starting_the_actor() {
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let fetches = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let (_tx, events) = crate::spider::tx::channel(MAX_EVENTS);
    let mut runtime = Runtime::new(Setup {
        scheduler: scheduler::Memory::new(),
        downloader: TestDownload {
            fetches: Arc::clone(&fetches),
            closes: Arc::clone(&closes),
        },
        executor: TestExecutor,
        store: TestStore,
        events,
        registry: middleware::Registry::new(),
        middlewares: Vec::new(),
    })
    .with_init(BlockingInit {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    });
    let (send_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        received
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    });

    let task = tokio::spawn(async move {
        let result = runtime.start_with_shutdown(false, &mut shutdown).await;
        (result, runtime.scheduler().queued_len())
    });
    started.notified().await;
    send_signal.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    release.notify_one();
    let (result, queued) = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("Engine did not close after the active init completed")
        .unwrap();
    result.unwrap();
    assert_eq!(queued, 1);
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn signal_drains_every_request_from_an_active_claim_without_claiming_again() {
    let claim_started = Arc::new(Notify::new());
    let release_claim = Arc::new(Notify::new());
    let claims = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let scheduler_closes = Arc::new(AtomicUsize::new(0));
    let fetches = Arc::new(AtomicUsize::new(0));
    let downloader_closes = Arc::new(AtomicUsize::new(0));
    let snapshot = Arc::new(crate::trace::Snapshot::code("task-1"));
    let requests = ["one", "two"]
        .into_iter()
        .map(|id| {
            let mut request = crate::net::Request::follow(format!("https://example.com/{id}"))
                .unwrap()
                .with_id(id);
            request.task_id = "task-1".to_string();
            request.trace_id = "trace-1".to_string();
            request.version = 1;
            request.state = crate::net::State::Processing;
            request.leased_by = "worker-1".to_string();
            request.set_snapshot(Arc::clone(&snapshot));
            request
        })
        .collect();
    let scheduler = ClaimScheduler {
        started: Arc::clone(&claim_started),
        release: Arc::clone(&release_claim),
        requests: std::sync::Mutex::new(Some(requests)),
        claims: Arc::clone(&claims),
        successes: Arc::clone(&successes),
        closes: Arc::clone(&scheduler_closes),
    };
    let (tx, events) = crate::spider::tx::channel(MAX_EVENTS);
    drop(tx);
    let mut runtime = Runtime::new(Setup {
        scheduler,
        downloader: TestDownload {
            fetches: Arc::clone(&fetches),
            closes: Arc::clone(&downloader_closes),
        },
        executor: TestExecutor,
        store: TestStore,
        events,
        registry: middleware::Registry::new(),
        middlewares: Vec::new(),
    });
    let (send_signal, received) = oneshot::channel::<()>();
    let mut shutdown: ShutdownSignal = Box::pin(async move {
        received
            .await
            .map_err(|error| crate::Error::message(error.to_string()))
    });

    let task = tokio::spawn(async move { runtime.start_with_shutdown(false, &mut shutdown).await });
    claim_started.notified().await;
    send_signal.send(()).unwrap();
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    release_claim.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("Engine did not drain the active claim after shutdown")
        .unwrap()
        .unwrap();
    assert_eq!(claims.load(Ordering::SeqCst), 1);
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    assert_eq!(successes.load(Ordering::SeqCst), 2);
    assert_eq!(downloader_closes.load(Ordering::SeqCst), 1);
    assert_eq!(scheduler_closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn start_until_idle_uses_the_configured_idle_interval() {
    let interval = Duration::from_millis(80);
    let (tx, events) = crate::spider::tx::channel(MAX_EVENTS);
    drop(tx);
    let mut runtime = Runtime::new(Setup {
        scheduler: IdleScheduler::new(),
        downloader: TestDownload {
            fetches: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
        },
        executor: TestExecutor,
        store: TestStore,
        events,
        registry: middleware::Registry::new(),
        middlewares: Vec::new(),
    })
    .with_idle_interval(interval);

    tokio::time::timeout(Duration::from_secs(2), runtime.start_until_idle())
        .await
        .expect("Engine did not stop after Scheduler became idle")
        .unwrap();

    let claims = runtime.scheduler().claims.lock().unwrap();
    assert_eq!(claims.len(), 2);
    assert!(
        claims[1].duration_since(claims[0]) >= interval,
        "claims were separated by {:?}, expected at least {interval:?}",
        claims[1].duration_since(claims[0])
    );
}

#[tokio::test]
async fn close_returns_the_first_error_and_logs_later_failures() {
    let (_tx, events) = crate::spider::tx::channel(MAX_EVENTS);
    let runtime = Runtime::new(Setup {
        scheduler: FailingScheduler,
        downloader: FailingDownload,
        executor: TestExecutor,
        store: FailingStore,
        events,
        registry: middleware::Registry::new(),
        middlewares: Vec::new(),
    });
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let result = runtime
        .close()
        .with_subscriber(RecordedEvents::new(Arc::clone(&recorded)))
        .await;

    assert!(matches!(
        result,
        Err(crate::Error::Download(downloader::Error::InvalidConfig(message)))
            if message == "downloader close failed"
    ));
    let recorded = recorded.lock().unwrap();
    assert!(recorded.iter().any(|event| {
        event.contains("additional Item Store close failure")
            && event.contains("store close failed")
    }));
    assert!(recorded.iter().any(|event| {
        event.contains("additional Scheduler close failure")
            && event.contains("scheduler close failed")
    }));
}

#[tokio::test]
async fn lifecycle_returns_the_first_error_and_logs_cleanup_failures() {
    let (tx, events) = crate::spider::tx::channel(MAX_EVENTS);
    drop(tx);
    let registry = middleware::Registry::new();
    registry.register("failing-lifecycle", FailingLifecycle);
    let mut runtime = Runtime::new(Setup {
        scheduler: IdleScheduler::new(),
        downloader: FailingDownload,
        executor: TestExecutor,
        store: TestStore,
        events,
        registry,
        middlewares: vec![middleware::Spec::new("failing-lifecycle")],
    });
    let mut shutdown: ShutdownSignal = Box::pin(std::future::pending());
    let recorded = Arc::new(Mutex::new(Vec::new()));

    let result = runtime
        .start_with_shutdown(true, &mut shutdown)
        .with_subscriber(RecordedEvents::new(Arc::clone(&recorded)))
        .await;

    assert!(matches!(
        result,
        Err(crate::Error::Middleware(middleware::Error::Message(message)))
            if message == "before_spider failed"
    ));
    let recorded = recorded.lock().unwrap();
    assert!(recorded.iter().any(|event| {
        event.contains("after_spider failed after Engine execution failed")
            && event.contains("after_spider failed")
    }));
    assert!(recorded.iter().any(|event| {
        event.contains("Engine close failed after execution failed")
            && event.contains("downloader close failed")
    }));
}
