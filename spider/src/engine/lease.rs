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

    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    let refresh = refresh(
        scheduler,
        payload,
        policy,
        claimed_deadline(request.lease_time, policy.timeout()),
    );
    tokio::pin!(execution);
    tokio::pin!(refresh);
    tokio::select! {
        biased;
        error = &mut refresh => Err(crate::Error::Scheduler(error)),
        result = &mut execution => result,
    }
}

/// Refreshes until the current execution no longer owns a valid lease.
async fn refresh<S>(
    scheduler: &S,
    payload: payload::Payload,
    policy: scheduler::Lease,
    mut deadline: tokio::time::Instant,
) -> scheduler::Error
where
    S: scheduler::Scheduler,
{
    loop {
        match refresh_until(scheduler, &payload, deadline, policy.interval()).await {
            Ok(refreshed_at) => deadline = refreshed_at + policy.timeout(),
            Err(error) => return error,
        }
        tokio::time::sleep(policy.interval()).await;
    }
}

async fn refresh_until<S>(
    scheduler: &S,
    payload: &payload::Payload,
    deadline: tokio::time::Instant,
    interval: Duration,
) -> Result<tokio::time::Instant, scheduler::Error>
where
    S: scheduler::Scheduler,
{
    loop {
        let started_at = tokio::time::Instant::now();
        let result = tokio::time::timeout_at(deadline, scheduler.refresh_lease(payload)).await;
        match result {
            Ok(Ok(())) => return Ok(started_at),
            Ok(Err(error)) if error.is_ownership_loss() => return Err(error),
            Ok(Err(error)) if error.is_transient() => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(scheduler::Error::LeaseExpired(payload.id.clone()));
                }
                tokio::time::sleep_until((now + RETRY_DELAY.min(interval)).min(deadline)).await;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(scheduler::Error::LeaseExpired(payload.id.clone())),
        }
    }
}

fn claimed_deadline(lease_time: i64, timeout: Duration) -> tokio::time::Instant {
    let elapsed = crate::utils::time::now_millis()
        .saturating_sub(lease_time)
        .max(0) as u64;
    tokio::time::Instant::now() + timeout.saturating_sub(Duration::from_millis(elapsed))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct TestScheduler {
        policy: scheduler::Lease,
        refreshes: AtomicUsize,
        successes: usize,
        first_delay: Duration,
    }

    impl TestScheduler {
        fn new(policy: scheduler::Lease) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: usize::MAX,
                first_delay: Duration::ZERO,
            }
        }

        fn unavailable(policy: scheduler::Lease) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: 0,
                first_delay: Duration::ZERO,
            }
        }

        fn slow_then_unavailable(policy: scheduler::Lease, delay: Duration) -> Self {
            Self {
                policy,
                refreshes: AtomicUsize::new(0),
                successes: 1,
                first_delay: delay,
            }
        }
    }

    impl scheduler::Scheduler for TestScheduler {
        fn lease(&self) -> Option<scheduler::Lease> {
            Some(self.policy)
        }

        async fn open(&self) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn push(&self, _payload: payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn push_items(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
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
        ) -> Result<Vec<net::Request>, scheduler::Error> {
            Ok(Vec::new())
        }

        async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
            Ok(false)
        }

        async fn ack(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn release(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn refresh_lease(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            let attempt = self.refreshes.fetch_add(1, Ordering::SeqCst);
            if attempt >= self.successes {
                return Err(scheduler::Error::Unavailable("offline".to_string()));
            }
            if attempt == 0 {
                tokio::time::sleep(self.first_delay).await;
            }
            Ok(())
        }

        async fn success(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }

        async fn failure(&self, _payload: &payload::Payload) -> Result<(), scheduler::Error> {
            Ok(())
        }
    }

    fn claimed_request() -> net::Request {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.version = 1;
        request.state = net::State::Processing;
        request.leased_by = "worker-1".to_string();
        request.lease_time = crate::utils::time::now_millis();
        request
    }

    #[tokio::test]
    async fn starts_refresh_before_immediate_execution_completes() {
        let policy =
            scheduler::Lease::new(Duration::from_secs(1), Duration::from_millis(500)).unwrap();
        let scheduler = TestScheduler::new(policy);
        let request = claimed_request();

        let result = run(&scheduler, &request, async { Ok::<_, crate::Error>(()) }).await;

        result.unwrap();
        assert_eq!(scheduler.refreshes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn slow_initial_refresh_does_not_pause_execution() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(200), Duration::from_millis(50)).unwrap();
        let scheduler = TestScheduler::slow_then_unavailable(policy, Duration::from_millis(100));
        let request = claimed_request();

        tokio::time::timeout(
            Duration::from_millis(25),
            run(&scheduler, &request, async { Ok::<_, crate::Error>(()) }),
        )
        .await
        .expect("lease refresh must run alongside request execution")
        .unwrap();

        assert_eq!(scheduler.refreshes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn initial_refresh_respects_the_claimed_lease_deadline() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(100), Duration::from_millis(50)).unwrap();
        let scheduler = TestScheduler::unavailable(policy);
        let mut request = claimed_request();
        request.lease_time -= 90;

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            run(
                &scheduler,
                &request,
                std::future::pending::<Result<(), crate::Error>>(),
            ),
        )
        .await
        .expect("the original claim deadline must bound refresh retries");

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::LeaseExpired(id))) if id == request.id
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn slow_refresh_does_not_extend_the_lease_from_response_time() {
        let policy =
            scheduler::Lease::new(Duration::from_millis(120), Duration::from_millis(20)).unwrap();
        let scheduler = TestScheduler::slow_then_unavailable(policy, Duration::from_millis(80));
        let request = claimed_request();

        let result = tokio::time::timeout(
            Duration::from_millis(160),
            run(
                &scheduler,
                &request,
                std::future::pending::<Result<(), crate::Error>>(),
            ),
        )
        .await
        .expect("a slow response must not move the lease deadline forward");

        assert!(matches!(
            result,
            Err(crate::Error::Scheduler(scheduler::Error::LeaseExpired(id))) if id == request.id
        ));
        assert!(scheduler.refreshes.load(Ordering::SeqCst) >= 2);
    }
}
