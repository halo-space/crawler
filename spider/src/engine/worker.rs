use std::sync::Arc;

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
        let result =
            engine::request::execute(&request, downloader, executor, registry, stats.clone()).await;

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
