use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use spider::scheduler::{Init, Scheduler};
use spider::{downloader, engine, net, payload};

struct Download;

impl downloader::Download for Download {
    async fn open(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), downloader::Error> {
        Ok(())
    }

    async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
        let mut response = net::Response::new(request, net::StatusCode(200), Bytes::new());
        response.reason = Some("OK".to_string());
        Ok(response)
    }
}

#[macros::spider]
struct RecoverySpider {
    calls: Arc<AtomicUsize>,
}

#[macros::spider]
impl RecoverySpider {
    fn name(&self) -> &str {
        "code-recovery"
    }

    async fn start(&self) -> Result<(), spider::Error> {
        Ok(())
    }

    async fn index(&self, _response: net::Response) -> Result<(), spider::Error> {
        Ok(())
    }

    async fn detail(&self, _response: net::Response) -> Result<(), spider::Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn traced_request(scheduler: &spider::Memory, node: &str) -> net::Request {
    scheduler
        .init(
            "trace-existing".to_string(),
            spider::trace::Snapshot::code("task-existing"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut request = net::Request::follow("https://example.com/detail").unwrap();
    request.task_id = "task-existing".to_string();
    request.trace_id = "trace-existing".to_string();
    request = request.node(node.to_string());
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    request
}

#[tokio::test]
async fn released_code_request_resolves_its_stable_node_again() {
    let scheduler = spider::Memory::new();
    traced_request(&scheduler, "detail").await;
    let claimed = scheduler
        .next_requests(1, "worker-1", &[net::Mode::Http])
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut release = payload::Payload::for_request(&claimed, "worker-1");
    release.state = net::State::Processing;
    scheduler.release(&release).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(Download)
        .with_spider(RecoverySpider::new(calls.clone()))
        .build();

    runtime.start().await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.scheduler().done_len(), 1);
}

#[tokio::test]
async fn missing_code_node_uses_request_failure_settlement() {
    let scheduler = spider::Memory::new();
    traced_request(&scheduler, "missing").await;
    let mut runtime = engine::Builder::new()
        .with_scheduler(scheduler)
        .with_downloader(Download)
        .with_spider(RecoverySpider::new(Arc::new(AtomicUsize::new(0))))
        .build();

    runtime.start().await.unwrap();

    assert_eq!(runtime.scheduler().done_len(), 0);
    assert_eq!(runtime.scheduler().failed_len(), 1);
    assert!(
        runtime
            .scheduler()
            .errors()
            .iter()
            .any(|error| error.contains("code node is not registered"))
    );
}
