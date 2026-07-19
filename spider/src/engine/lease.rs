use std::future::Future;
use std::time::Duration;

use crate::{net, payload, scheduler};

const RETRY_DELAY: Duration = Duration::from_millis(25);

/// Runs one Request execution while its acknowledged lease remains valid.
///
/// Ownership loss, lease expiry, or a terminal refresh error stops the
/// execution and returns a Scheduler error.
pub(super) async fn run<S, T>(
    scheduler: &S,
    request: &net::Request,
    execution: impl Future<Output = Result<T, crate::Error>>,
) -> Result<T, crate::Error>
where
    S: scheduler::Scheduler,
{
    let Some(policy) = scheduler.lease() else {
        return execution.await;
    };

    let refresh = refresh(scheduler, request, policy);
    tokio::pin!(execution);
    tokio::pin!(refresh);
    tokio::select! {
        biased;
        result = &mut execution => result,
        error = &mut refresh => Err(crate::Error::Scheduler(error)),
    }
}

/// Refreshes until the current execution no longer owns a valid lease.
async fn refresh<S>(
    scheduler: &S,
    request: &net::Request,
    policy: scheduler::Lease,
) -> scheduler::Error
where
    S: scheduler::Scheduler,
{
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    let mut deadline = tokio::time::Instant::now() + policy.timeout();

    loop {
        tokio::time::sleep(policy.interval()).await;
        loop {
            let result = tokio::time::timeout_at(deadline, scheduler.refresh_lease(&payload)).await;
            match result {
                Ok(Ok(())) => {
                    deadline = tokio::time::Instant::now() + policy.timeout();
                    break;
                }
                Ok(Err(error)) if error.is_ownership_loss() => return error,
                Ok(Err(error)) if error.is_transient() => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return scheduler::Error::LeaseExpired(payload.id.clone());
                    }
                    let delay = RETRY_DELAY.min(policy.interval());
                    tokio::time::sleep_until((now + delay).min(deadline)).await;
                }
                Ok(Err(error)) => return error,
                Err(_) => return scheduler::Error::LeaseExpired(payload.id.clone()),
            }
        }
    }
}
