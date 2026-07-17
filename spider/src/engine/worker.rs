use std::sync::Arc;

use crate::{downloader, engine, middleware, payload, scheduler, stats};

const MAX_SCHEDULER_ATTEMPTS: usize = 3;
const SCHEDULER_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

pub(super) async fn run<S, D, R>(
    request: crate::net::Request,
    scheduler: Arc<S>,
    downloader: Arc<D>,
    executor: Arc<R>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + 'static,
    D: downloader::Download + 'static,
    R: engine::contract::Execute + 'static,
{
    let mut ack = payload::Payload::for_request(&request, request.leased_by.clone());
    ack.state = crate::net::State::Processing;
    if let Err(error) = retry_ack(scheduler.as_ref(), &ack).await {
        if !error.is_ownership_loss() {
            let _ = retry_release(scheduler.as_ref(), &ack).await;
        }
        return Err(crate::Error::Scheduler(error));
    }

    let start_time = crate::utils::time::now_millis();
    let delta = Arc::new(stats::Delta::default());
    delta.total(request.node_key(), 1);
    let lifecycle = async {
        let execution =
            engine::request::execute(&request, downloader, executor, registry, delta.clone()).await;

        let mut payload = payload::Payload::for_request(&request, request.leased_by.clone());
        payload.start_time = Some(start_time);
        payload.end_time = Some(crate::utils::time::now_millis());
        if let Err(error) = execution {
            payload = payload.failed(format!("{}: {error}", request.url));
        } else {
            delta.done(request.node_key(), 1);
        }
        payload.stats = delta.snapshot();

        if payload.state == crate::net::State::Failed {
            retry_failure(scheduler.as_ref(), &payload).await
        } else {
            retry_success(scheduler.as_ref(), &payload).await
        }
    };
    engine::lease::maintain(scheduler.as_ref(), &request, lifecycle).await
}

async fn retry_success<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), crate::Error> {
    for attempt in 0..MAX_SCHEDULER_ATTEMPTS {
        match scheduler.success(payload).await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_transient() && attempt + 1 < MAX_SCHEDULER_ATTEMPTS => {
                tokio::time::sleep(SCHEDULER_RETRY_DELAY).await;
            }
            Err(error) => return Err(crate::Error::Scheduler(error)),
        }
    }
    unreachable!()
}

async fn retry_failure<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), crate::Error> {
    for attempt in 0..MAX_SCHEDULER_ATTEMPTS {
        match scheduler.failure(payload).await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_transient() && attempt + 1 < MAX_SCHEDULER_ATTEMPTS => {
                tokio::time::sleep(SCHEDULER_RETRY_DELAY).await;
            }
            Err(error) => return Err(crate::Error::Scheduler(error)),
        }
    }
    unreachable!()
}

async fn retry_ack<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), scheduler::Error> {
    retry_operation(|| scheduler.ack(payload)).await
}

async fn retry_release<S: scheduler::Scheduler>(
    scheduler: &S,
    payload: &payload::Payload,
) -> Result<(), scheduler::Error> {
    retry_operation(|| scheduler.release(payload)).await
}

async fn retry_operation<F, Fut>(mut operation: F) -> Result<(), scheduler::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), scheduler::Error>>,
{
    for attempt in 0..MAX_SCHEDULER_ATTEMPTS {
        match operation().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_transient() && attempt + 1 < MAX_SCHEDULER_ATTEMPTS => {
                tokio::time::sleep(SCHEDULER_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!()
}
