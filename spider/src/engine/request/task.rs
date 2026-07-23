use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;

use crate::{downloader, engine, middleware, payload, scheduler, stats};

const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

enum Ack {
    Accepted,
    Expired,
}

/// Executes and settles one claimed Request.
pub(in crate::engine) async fn execute<S, D, E>(
    request: crate::net::Request,
    claim_started: tokio::time::Instant,
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
    let expires_at =
        engine::lease::expires_at(scheduler.lease(), claim_started, request.id.as_str())
            .map_err(crate::Error::Scheduler)?;
    match acknowledge(scheduler.as_ref(), &payload, expires_at).await {
        Ok(Ack::Accepted) => {}
        Ok(Ack::Expired) => return release_unstarted(scheduler.as_ref(), &payload).await,
        Err(error) => {
            if !error.is_ownership_loss() {
                let _ = retry_release(scheduler.as_ref(), &payload).await;
            }
            return Err(crate::Error::Scheduler(error));
        }
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
            payload = payload.failed(format!(
                "request {} at node {} failed: {error}",
                request.id,
                request.node_key()
            ));
        } else {
            stats.done(request.node_key(), 1);
        }
        payload.stats = stats.snapshot();
        payload
    };
    let settlement_scheduler = scheduler.clone();
    engine::lease::execute_with_lease(
        scheduler.as_ref(),
        &request,
        claim_started,
        execution,
        move |payload| async move {
            if payload.state == crate::net::State::Failed {
                retry_failure(settlement_scheduler.as_ref(), &payload).await
            } else {
                retry_success(settlement_scheduler.as_ref(), &payload).await
            }
        },
    )
    .await
}

async fn acknowledge<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
    expires_at: Option<tokio::time::Instant>,
) -> Result<Ack, scheduler::Error> {
    let Some(expires_at) = expires_at else {
        retry_ack(scheduler, payload).await?;
        return Ok(Ack::Accepted);
    };
    if tokio::time::Instant::now() >= expires_at {
        return Ok(Ack::Expired);
    }

    match tokio::time::timeout_at(expires_at, retry_ack(scheduler, payload)).await {
        Ok(Ok(())) if tokio::time::Instant::now() < expires_at => Ok(Ack::Accepted),
        Ok(Ok(())) | Err(_) => Ok(Ack::Expired),
        Ok(Err(error)) => Err(error),
    }
}

async fn release_unstarted<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), crate::Error> {
    match retry_release(scheduler, payload).await {
        Ok(()) => Ok(()),
        Err(error) if error.is_ownership_loss() => Ok(()),
        Err(error) => Err(crate::Error::Scheduler(error)),
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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

    struct CountingDownload {
        calls: Arc<AtomicUsize>,
    }

    impl downloader::Download for CountingDownload {
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
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(downloader::Error::UnsupportedMode(
                "the expired acknowledgement must not download".to_string(),
            ))
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
        let scheduler = Arc::new(scheduler::Memory::new());
        let request = crate::net::Request::follow(
            "https://user:password@example.com/private?api_key=url-secret",
        )
        .unwrap();
        scheduler
            .push(payload::Payload::new().requests(vec![request]))
            .await
            .unwrap();
        let request = scheduler
            .next_requests(1, "worker-1", &[crate::net::Mode::Http])
            .await
            .unwrap()
            .pop()
            .unwrap();

        execute(
            request,
            tokio::time::Instant::now(),
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
        let errors = scheduler.errors().join("\n");
        for secret in ["user", "password", "api_key", "url-secret"] {
            assert!(
                !errors.contains(secret),
                "failure settlement exposed {secret}: {errors}"
            );
        }
    }

    #[tokio::test]
    async fn expired_acknowledgement_releases_without_downloading_or_retrying() {
        let lease =
            scheduler::Lease::new(Duration::from_millis(10), Duration::from_millis(1)).unwrap();
        let scheduler = Arc::new(scheduler::Memory::new().with_lease(lease));
        let request = crate::net::Request::follow("https://example.com").unwrap();
        let request_id = request.id.clone();
        scheduler
            .push(payload::Payload::new().requests(vec![request]))
            .await
            .unwrap();
        let claimed = scheduler
            .next_requests(1, "worker-1", &[crate::net::Mode::Http])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));

        execute(
            claimed,
            tokio::time::Instant::now() - Duration::from_millis(20),
            scheduler.clone(),
            Arc::new(CountingDownload {
                calls: calls.clone(),
            }),
            Arc::new(Executor),
            Arc::new(middleware::Registry::new()),
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(scheduler.processing_len(), 0);
        assert_eq!(scheduler.queued_len(), 1);

        let reclaimed = scheduler
            .next_requests(1, "worker-1", &[crate::net::Mode::Http])
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(reclaimed.id, request_id);
        assert_eq!(reclaimed.retry_count, 0);
    }
}
