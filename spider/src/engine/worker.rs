use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;

use crate::{downloader, engine, middleware, payload, scheduler, stats};

const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

/// Executes and settles one claimed Request.
pub(super) async fn run<S, D, E>(
    request: crate::net::Request,
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<E>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    E: engine::contract::Execute + 'static,
{
    let mut payload = payload::Payload::for_request(&request, request.leased_by.clone());
    payload.state = crate::net::State::Processing;
    if let Err(error) = retry_ack(scheduler.as_ref(), &payload).await {
        if !error.is_ownership_loss() {
            let _ = retry_release(scheduler.as_ref(), &payload).await;
        }
        return Err(crate::Error::Scheduler(error));
    }

    let start_time = crate::utils::time::now_millis();
    let stats = Arc::new(stats::Delta::default());
    stats.total(request.node_key(), 1);
    let execution = async {
        let result = AssertUnwindSafe(engine::request::execute(
            &request,
            downloader,
            executor,
            registry,
            stats.clone(),
        ))
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| Err(crate::Error::message(panic_message(panic))));

        let mut payload = payload::Payload::for_request(&request, request.leased_by.clone());
        payload.start_time = Some(start_time);
        payload.end_time = Some(crate::utils::time::now_millis());
        if let Err(error) = result {
            payload = payload.failed(format!("{}: {error}", request.url));
        } else {
            stats.done(request.node_key(), 1);
        }
        payload.stats = stats.snapshot();

        if payload.state == crate::net::State::Failed {
            retry_failure(scheduler.as_ref(), &payload).await
        } else {
            retry_success(scheduler.as_ref(), &payload).await
        }
    };
    engine::lease::run(scheduler.as_ref(), &request, execution).await
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        format!("request execution panicked: {message}")
    } else if let Some(message) = panic.downcast_ref::<String>() {
        format!("request execution panicked: {message}")
    } else {
        "request execution panicked".to_string()
    }
}

async fn retry_success<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), crate::Error> {
    retry(|| scheduler.success(payload))
        .await
        .map_err(crate::Error::Scheduler)
}

async fn retry_failure<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), crate::Error> {
    retry(|| scheduler.failure(payload))
        .await
        .map_err(crate::Error::Scheduler)
}

async fn retry_ack<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), scheduler::Error> {
    retry(|| scheduler.ack(payload)).await
}

async fn retry_release<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), scheduler::Error> {
    retry(|| scheduler.release(payload)).await
}

async fn retry<Fut>(mut operation: impl FnMut() -> Fut) -> Result<(), scheduler::Error>
where
    Fut: std::future::Future<Output = Result<(), scheduler::Error>>,
{
    for attempt in 0..MAX_ATTEMPTS {
        match operation().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_transient() && attempt + 1 < MAX_ATTEMPTS => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::Scheduler;

    struct PanickingDownload;

    impl downloader::Download for PanickingDownload {
        async fn open(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn fetch(
            &self,
            _request: crate::net::Request,
        ) -> Result<crate::net::Response, downloader::Error> {
            panic!("download exploded")
        }
    }

    struct Executor;

    impl engine::contract::Execute for Executor {
        async fn allowed_domains(&self, _request: &crate::net::Request) -> Vec<String> {
            Vec::new()
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

    #[tokio::test]
    async fn request_panic_is_settled_as_failure() {
        let scheduler = Arc::new(scheduler::Memory::new("worker-1"));
        let request = crate::net::Request::follow("https://example.com").unwrap();
        scheduler
            .push(payload::Payload::new().requests(vec![request]))
            .await
            .unwrap();
        let request = scheduler.next_requests(1).await.unwrap().pop().unwrap();

        run(
            request,
            scheduler.clone(),
            Arc::new(PanickingDownload),
            Arc::new(Executor),
            Arc::new(middleware::Registry::new()),
        )
        .await
        .unwrap();

        assert_eq!(scheduler.processing_len(), 0);
        assert_eq!(scheduler.failed_len(), 1);
        assert!(
            scheduler
                .errors()
                .iter()
                .any(|error| error.contains("request execution panicked: download exploded"))
        );
    }
}
